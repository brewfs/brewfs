//! Actual Redis/TiKV catalog transactions plus real VFS native data and actual
//! LocalFS producer PUTs. The wrapper schedules actual claim/final CAS delivery.

#[path = "carrier_basis_tests.rs"]
mod carrier_basis_tests;
#[path = "fork_alias_tests.rs"]
mod fork_alias_tests;
#[path = "history_retirement_tests.rs"]
mod history_retirement_tests;
#[path = "native_lease_reaper_tests.rs"]
mod native_lease_reaper_tests;
#[path = "prepare_tests.rs"]
mod prepare_tests;
#[path = "process_restart_tests.rs"]
mod process_restart_tests;
#[path = "quiesced_seed_takeover_tests.rs"]
mod quiesced_seed_takeover_tests;
#[path = "recovery_tests.rs"]
mod recovery_tests;
#[path = "root_conflict_tests.rs"]
mod root_conflict_tests;

use super::*;
use crate::chunk::ChunkLayout;
use crate::chunk::read_plan::{WorkspaceReadPlanProvider, execute_unified_into};
use crate::chunk::store::InMemoryBlockStore;
use crate::meta::layer::MetaLayer;
use crate::vfs::config::VFSConfig;
use crate::vfs::fs::VFS;
use crate::workspace_overlay::catalog::{RenewLease, WorkspaceStore};
use crate::workspace_overlay::meta_layer::{
    PinnedCatalogPackedBindingAuthority, WorkspaceMetaLayer,
};
use crate::workspace_overlay::model::ViewContext;
use crate::workspace_overlay::packed_reader_lifecycle::PackedReaderLeaseOptions;
use crate::workspace_overlay::packed_v3::wire005::{
    AuthenticatedV3Snapshot, NativeCaptureLimits, V3IndexReader, V3ProducerOptions,
};
use crate::workspace_overlay::packed_v3::{
    AccessProfile, PackedCodec, PackedV3ReadonlyMeta, SizeClassTable,
};
use crate::workspace_overlay::stores::binding_tests::{packed, request};
use crate::workspace_overlay::stores::kv_backend::KvEntry;
use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
use crate::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum Delivery {
    Normal = 0,
    LostReply = 1,
    CancelWaiter = 2,
    UnknownBefore = 3,
    HoldFinalBefore = 4,
    HistoryRetirement = 5,
    CarrierFinalFalse = 6,
    CarrierCounterfactual = 7,
    ForkAliases = 8,
}

type CatalogPacket = (Vec<KvCheck>, Vec<KvWrite>);
type TimedCatalogPacket = (Vec<KvCheck>, Vec<KvWrite>, i64);
type CatalogTimeWindowChecks = (Vec<KvCheck>, Option<i64>, Option<i64>);
type ForkConnection<B> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<B, WorkspaceError>> + Send>>;
type ForkConnect<B> = Arc<dyn Fn(Arc<V3MountBudget>) -> ForkConnection<B> + Send + Sync>;

struct FinalDelivery<B> {
    inner: Arc<B>,
    mode: AtomicU8,
    final_calls: AtomicUsize,
    claim_mode: AtomicU8,
    claim_calls: AtomicUsize,
    stop_original_seed_q: AtomicBool,
    fork_pause_quiesced: AtomicBool,
    fork_quiesced_entered: tokio::sync::Semaphore,
    fork_quiesced_release: tokio::sync::Semaphore,
    seed_prepare_checks: std::sync::Mutex<Option<Vec<KvCheck>>>,
    seed_first_pnb: std::sync::Mutex<Option<TimedCatalogPacket>>,
    claim_checks: std::sync::Mutex<Option<CatalogTimeWindowChecks>>,
    final_checks: std::sync::Mutex<Option<(Vec<KvCheck>, i64)>>,
    final_writes: std::sync::Mutex<Option<Vec<KvWrite>>>,
    confirmation_reads: std::sync::Mutex<Vec<(Vec<Vec<u8>>, KvReadLimits)>>,
    confirmation_proof: std::sync::Mutex<Option<(Vec<KvCheck>, i64)>>,
    confirmation_calls: AtomicUsize,
    fork_packets: std::sync::Mutex<Vec<CatalogPacket>>,
    alias_retirement_packets: std::sync::Mutex<Vec<CatalogPacket>>,
    native_quarantine_packets: std::sync::Mutex<Vec<CatalogPacket>>,
    carrier_cleanup_calls: AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    done: tokio::sync::Notify,
}
impl<B> FinalDelivery<B> {
    fn new(inner: Arc<B>) -> Self {
        Self {
            inner,
            mode: AtomicU8::new(0),
            final_calls: AtomicUsize::new(0),
            claim_mode: AtomicU8::new(0),
            claim_calls: AtomicUsize::new(0),
            stop_original_seed_q: AtomicBool::new(false),
            fork_pause_quiesced: AtomicBool::new(false),
            fork_quiesced_entered: tokio::sync::Semaphore::new(0),
            fork_quiesced_release: tokio::sync::Semaphore::new(0),
            seed_prepare_checks: std::sync::Mutex::new(None),
            seed_first_pnb: std::sync::Mutex::new(None),
            claim_checks: std::sync::Mutex::new(None),
            final_checks: std::sync::Mutex::new(None),
            final_writes: std::sync::Mutex::new(None),
            confirmation_reads: std::sync::Mutex::new(Vec::new()),
            confirmation_proof: std::sync::Mutex::new(None),
            confirmation_calls: AtomicUsize::new(0),
            fork_packets: std::sync::Mutex::new(Vec::new()),
            alias_retirement_packets: std::sync::Mutex::new(Vec::new()),
            native_quarantine_packets: std::sync::Mutex::new(Vec::new()),
            carrier_cleanup_calls: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            done: tokio::sync::Notify::new(),
        }
    }
    fn final_write(writes: &[KvWrite]) -> bool {
        writes.iter().any(|write| match write {
            KvWrite::Put { key, value } if key.starts_with(JOURNAL_PREFIX) => {
                PackedJournalRecord::decode(value)
                    .is_ok_and(|record| record.phase == PackedJournalPhase::Committed)
            }
            _ => false,
        })
    }
}

#[async_trait]
impl<B: WorkspaceKvBackend> WorkspaceKvBackend for FinalDelivery<B> {
    fn supports_consistent_reads(&self) -> bool {
        self.inner.supports_consistent_reads()
    }
    fn name(&self) -> &'static str {
        self.inner.name()
    }
    async fn shutdown_metadata_backend(&self) -> Result<(), WorkspaceError> {
        self.inner.shutdown_metadata_backend().await
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        self.inner.get(key).await
    }
    async fn get_many(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.inner.get_many(keys).await
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.inner.get_many_consistent(keys).await
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.inner.get_many_consistent_with_time(keys).await
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        if self.mode.load(Ordering::SeqCst) == Delivery::LostReply as u8
            && self.final_calls.load(Ordering::SeqCst) > 0
            && self.confirmation_proof.lock().unwrap().is_none()
        {
            self.confirmation_reads
                .lock()
                .unwrap()
                .push((keys.to_vec(), limits));
        }
        self.inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await
    }
    async fn get_publication_packet_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        if self.mode.load(Ordering::SeqCst) == Delivery::LostReply as u8
            && self.final_calls.load(Ordering::SeqCst) > 0
            && self.confirmation_proof.lock().unwrap().is_none()
        {
            self.confirmation_reads
                .lock()
                .unwrap()
                .push((keys.to_vec(), limits));
        }
        self.inner
            .get_publication_packet_consistent_with_time_bounded(keys, limits)
            .await
    }
    async fn get_many_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.inner.get_many_with_time(keys).await
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner.scan_prefix(prefix).await
    }
    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        max: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner.scan_prefix_bounded(prefix, max).await
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner
            .scan_prefix_with_byte_limits(prefix, limits)
            .await
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        if fork_alias_tests::is_carrier_cleanup(writes) {
            fork_alias_tests::assert_carrier_cleanup_packet(checks, writes);
            self.carrier_cleanup_calls.fetch_add(1, Ordering::SeqCst);
        } else {
            carrier_basis_tests::assert_no_standalone_writes(writes, Self::final_write(writes));
        }
        if fork_alias_tests::is_creation(writes) {
            fork_alias_tests::assert_creation_packet(checks, writes);
            self.fork_packets
                .lock()
                .unwrap()
                .push((checks.to_vec(), writes.to_vec()));
        }
        self.inner.compare_and_swap(checks, writes).await
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        carrier_basis_tests::assert_no_standalone_writes(writes, Self::final_write(writes));
        if fork_alias_tests::is_logical_retirement(writes) {
            fork_alias_tests::assert_logical_retirement_packet(checks, writes);
            self.alias_retirement_packets
                .lock()
                .unwrap()
                .push((checks.to_vec(), writes.to_vec()));
        }
        let is_claim = writes.iter().any(|write| {
            matches!(write,
            KvWrite::Put { key, .. } if key.starts_with(b"packed/v3/native-recovery-claim/"))
        });
        if is_claim {
            self.claim_calls.fetch_add(1, Ordering::SeqCst);
            *self.claim_checks.lock().unwrap() = Some((checks.to_vec(), lower, upper));
        }
        let result = self
            .inner
            .compare_and_swap_in_time_window(checks, writes, lower, upper)
            .await?;
        if is_claim && result {
            match self.claim_mode.load(Ordering::SeqCst) {
                1 => {
                    return Err(WorkspaceError::Backend(
                        "actual committed recovery claim reply lost".into(),
                    ));
                }
                2 => {
                    self.entered.notify_one();
                    self.release.notified().await;
                    self.done.notify_one();
                }
                _ => {}
            }
        }
        Ok(result)
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        expires_at_ns: i64,
        limits: crate::workspace_overlay::stores::kv_backend::KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        crate::workspace_overlay::stores::kv_backend::validate_bounded_authentication_checks(
            checks, limits,
        )?;
        crate::workspace_overlay::stores::kv_backend::validate_cas_time_window(
            None,
            Some(expires_at_ns),
        )?;
        if self.mode.load(Ordering::SeqCst) == Delivery::LostReply as u8
            && self.final_calls.load(Ordering::SeqCst) > 0
        {
            self.confirmation_calls.fetch_add(1, Ordering::SeqCst);
            *self.confirmation_proof.lock().unwrap() = Some((checks.to_vec(), expires_at_ns));
        }
        self.inner
            .authenticate_checks_before_bounded(checks, expires_at_ns, limits)
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        let seed_write = writes.iter().any(|write|
            matches!(write, KvWrite::Put { key, .. } if key.starts_with(b"packed/v3/native-freeze-basis/")));
        let seal_journals = writes
            .iter()
            .filter_map(|write| match write {
                KvWrite::Put { key, value } if key.starts_with(HOT_JOURNAL_PREFIX) => {
                    let journal = decode_open_value::<SealJournal>(value, 48 << 10).ok()?;
                    (hot_journal_key(journal.workspace_id, journal.journal_id) == *key)
                        .then_some(journal)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if seed_write
            && seal_journals
                .iter()
                .any(|journal| journal.phase == SealPhase::Quiesced)
            && self.stop_original_seed_q.swap(false, Ordering::SeqCst)
        {
            // Prepare already committed on the actual backend. Stop before
            // submitting Q; no raw record or fabricated source is installed.
            return Err(WorkspaceError::Backend(
                "actual Prepare survived interrupted Q submission".into(),
            ));
        }
        let is_prepare = seed_write
            && seal_journals
                .iter()
                .any(|journal| journal.phase == SealPhase::Prepare);
        let is_first_pnb = writes.iter().any(|write| match write {
            KvWrite::Put { key, value } if key.starts_with(JOURNAL_PREFIX) => {
                PackedJournalRecord::decode(value).is_ok_and(|record| {
                    record.phase == PackedJournalPhase::Building
                        && record.native_rebind.is_some()
                        && *key == journal_key(record.journal_id)
                        && checks
                            .iter()
                            .any(|check| check.key == *key && check.expected.is_none())
                })
            }
            _ => false,
        });
        let is_final = Self::final_write(writes);
        carrier_basis_tests::assert_no_standalone_writes(writes, is_final);
        let mode = self.mode.load(Ordering::SeqCst);
        if writes.is_empty()
            && mode == Delivery::LostReply as u8
            && self.final_calls.load(Ordering::SeqCst) > 0
        {
            self.confirmation_calls.fetch_add(1, Ordering::SeqCst);
            *self.confirmation_proof.lock().unwrap() = Some((checks.to_vec(), deadline));
        }
        if is_final {
            self.final_calls.fetch_add(1, Ordering::SeqCst);
            *self.final_checks.lock().unwrap() = Some((checks.to_vec(), deadline));
            *self.final_writes.lock().unwrap() = Some(writes.to_vec());
            carrier_basis_tests::assert_packet(checks, writes);
            if mode == Delivery::CarrierFinalFalse as u8 {
                carrier_basis_tests::inject_descriptor_conflict(
                    self.inner.as_ref(),
                    checks,
                    writes,
                )
                .await;
            }
            if mode == Delivery::UnknownBefore as u8 {
                return Err(WorkspaceError::Backend(
                    "injected unknown final response before submission".into(),
                ));
            }
            if mode == Delivery::HoldFinalBefore as u8 {
                self.entered.notify_one();
                self.release.notified().await;
            }
        }
        let result = self
            .inner
            .compare_and_swap_before(checks, writes, deadline)
            .await?;
        if result
            && writes.iter().any(|write| match write {
                KvWrite::Put { key, value } if key.starts_with(JOURNAL_PREFIX) => {
                    PackedJournalRecord::decode(value).is_ok_and(|record| {
                        record.phase == PackedJournalPhase::Aborted
                            && record.native_rebind.is_some()
                    })
                }
                _ => false,
            })
        {
            self.native_quarantine_packets
                .lock()
                .unwrap()
                .push((checks.to_vec(), writes.to_vec()));
        }
        if result
            && seed_write
            && seal_journals
                .iter()
                .any(|journal| journal.phase == SealPhase::Quiesced)
            && self.fork_pause_quiesced.swap(false, Ordering::SeqCst)
        {
            // Observe an actual committed Quiesced journal; only delay its
            // original response. No source, journal or success is synthesized.
            self.fork_quiesced_entered.add_permits(1);
            self.fork_quiesced_release.acquire().await.unwrap().forget();
        }
        if result && is_prepare {
            *self.seed_prepare_checks.lock().unwrap() = Some(checks.to_vec());
        }
        if result && is_first_pnb {
            *self.seed_first_pnb.lock().unwrap() =
                Some((checks.to_vec(), writes.to_vec(), deadline));
        }
        if is_final && result {
            if mode == Delivery::LostReply as u8 {
                return Err(WorkspaceError::Backend(
                    "injected committed final response loss".into(),
                ));
            }
            if mode == Delivery::CancelWaiter as u8 {
                self.entered.notify_one();
                self.release.notified().await;
                self.done.notify_one();
            }
        }
        Ok(result)
    }
}

fn producer_options() -> V3ProducerOptions {
    V3ProducerOptions {
        snapshot_id: [0x51; 32],
        root_dir_key: [0x52; 32],
        root_inode: 1,
        profile: AccessProfile::RandomSmallFile,
        size_classes: SizeClassTable::default(),
        build_policy: Default::default(),
        metadata_codec: PackedCodec::Raw,
        data_codec: PackedCodec::Raw,
    }
}
fn capture_limits() -> NativeCaptureLimits {
    NativeCaptureLimits {
        max_inodes: 100,
        max_names: 100,
        max_spans: 1000,
        max_logical_bytes: 1 << 20,
        max_data_bytes: 1 << 20,
        max_payload_disk_bytes: 8 << 20,
        max_sqlite_disk_bytes: 8 << 20,
        max_producer_spool_disk_bytes: 8 << 20,
        sqlite_cache_bytes: 64 << 10,
        max_sql_vm_steps: 1_000_000,
    }
}
fn preparation_error<K, S>(error: &NativePublicationPreparationFailure<K, S>) -> &WorkspaceError
where
    K: WorkspaceKvBackend,
    S: BlockStore + Send + Sync + 'static,
{
    match error {
        NativePublicationPreparationFailure::BeforeHashed(error)
        | NativePublicationPreparationFailure::HashedAdmission { error, .. }
        | NativePublicationPreparationFailure::Hashed { error, .. } => error,
        NativePublicationPreparationFailure::Promotion { failure, .. } => &failure.error,
    }
}

async fn contract<B: WorkspaceKvBackend>(
    backend: Arc<B>,
    delivery: Delivery,
    fork_context: Option<(Arc<V3MountBudget>, ForkConnect<B>)>,
) {
    let (_objects, client, snapshot, lower_proof, _lower_data) = packed().await;
    let scratch = tempfile::tempdir().unwrap();
    let delivery_backend = Arc::new(FinalDelivery::new(backend));
    let budget = fork_context
        .as_ref()
        .map_or_else(V3MountBudget::defaults, |(budget, _)| budget.clone());
    let store = Arc::new(
        KvWorkspaceStore::from_arc(delivery_backend.clone())
            .with_packed_reader_pin_budget(budget.clone()),
    );
    let install = request(store.as_ref(), lower_proof).await;
    let old_binding = store
        .install_packed_lower_binding(install.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: old_binding.head_epoch,
        ..install.guard
    };
    store
        .renew_lease(RenewLease {
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
            ttl_ns: 300_000_000_000,
        })
        .await
        .unwrap();
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
    let lower = Arc::new(
        PackedV3ReadonlyMeta::from_v3_budget(client.clone(), snapshot, 4096, 0, budget.clone())
            .unwrap(),
    );
    let upper = Arc::new(InMemoryBlockStore::new());
    let layout = ChunkLayout {
        chunk_size: 4096,
        block_size: 4096,
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
            4096,
        )
        .with_packed_v3_lower(
            old_binding.binding.clone(),
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
    let inode = vfs.create_file("/factory-native").await.unwrap();
    let handle = vfs
        .open(
            inode,
            vfs.stat("/factory-native").await.unwrap(),
            true,
            true,
            false,
        )
        .await
        .unwrap();
    let native_payload = b"native factory actual bytes";
    assert_eq!(
        vfs.write(handle, 0, native_payload).await.unwrap(),
        native_payload.len()
    );
    vfs.flush(handle).await.unwrap();
    vfs.close(handle).await.unwrap();
    let local = vfs.quiesce_packed_vfs().await.unwrap();
    let layers: [LayerRecord; 2] = store
        .load_layer_chain(guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let old_base = layers[1].clone();
    let native = Arc::new(
        store
            .clone()
            .begin_packed_native_quiesce(
                guard.clone(),
                layers,
                JournalId::new(),
                LayerId::new(),
                budget.clone(),
            )
            .await
            .unwrap(),
    );
    let original_q = native.canonical_receipt_bytes().to_vec();
    let artifact = FrozenNativeArtifact::capture(
        native,
        local,
        scratch.path().to_path_buf(),
        capture_limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut ready = match store
        .prepare_frozen_native_publication(
            artifact,
            client.clone(),
            NativePublicationBuildOptions {
                producer: producer_options(),
                temporary: scratch.path().to_path_buf(),
                graph_scratch: scratch.path().to_path_buf(),
                graph_limits: V3IndexAuditLimits::default(),
                native_hash_limits: NativeDeltaHashLimits {
                    max_native_delta_rows: 1000,
                    max_canonical_bytes: 8 << 20,
                },
                chunk_size: 4096,
                metadata_cache_bytes: 0,
                max_catalog_rows: 100,
                cancel: CancellationToken::new(),
            },
        )
        .await
    {
        Ok(ready) => ready,
        Err(error) => panic!(
            "actual native factory preparation failed: {}",
            preparation_error(&error)
        ),
    };
    assert_eq!(
        ready.source.phase_authority().canonical_receipt_bytes(),
        original_q
    );
    assert_eq!(ready.record.phase, PackedJournalPhase::Verified);
    let plan = ready
        .record
        .native_rebind
        .as_ref()
        .unwrap()
        .publication
        .as_ref()
        .unwrap()
        .clone();
    let staged_record = ready.record.value.clone();
    let target = ready.record.commit_target.clone().unwrap();
    let source_digest = ready
        .source
        .phase_authority()
        .native_delta_hash()
        .delta_digest();
    let source_root = ready
        .source
        .phase_authority()
        .native_delta_hash()
        .root_hash();
    assert!(
        delivery_backend
            .get(&hot_layer_key(plan.carrier_layer_id))
            .await
            .unwrap()
            .is_none()
    );
    delivery_backend
        .mode
        .store(delivery as u8, Ordering::SeqCst);
    if matches!(delivery, Delivery::CarrierCounterfactual) {
        ready
            .record
            .value
            .commit_target
            .as_mut()
            .unwrap()
            .base_revision = BaseRevision {
            layer_id: guard.expected_head_layer_id,
            sealed_version: plan.source_sealed_version,
            root_hash: source_root,
        };
        let failure = match ready.commit().await {
            Ok(_) => {
                panic!("native-source revision substituted for carrier unexpectedly committed")
            }
            Err(failure) => failure,
        };
        assert!(failure.attempted_successor.is_none());
        assert_eq!(delivery_backend.final_calls.load(Ordering::SeqCst), 0);
        carrier_basis_tests::assert_uncommitted(
            delivery_backend.as_ref(),
            &guard,
            &old_binding,
            &target,
            &staged_record,
            false,
        )
        .await;
        assert!(failure.publication.commit().await.is_err());
        assert_eq!(delivery_backend.final_calls.load(Ordering::SeqCst), 0);
        meta.shutdown_session().await.unwrap();
        return;
    }
    if matches!(
        delivery,
        Delivery::UnknownBefore | Delivery::CarrierFinalFalse
    ) {
        let failure = match ready.commit().await {
            Ok(_) => panic!("unknown-before unexpectedly committed"),
            Err(failure) => failure,
        };
        assert_eq!(
            failure.attempted_successor.is_some(),
            matches!(delivery, Delivery::UnknownBefore)
        );
        carrier_basis_tests::assert_uncommitted(
            delivery_backend.as_ref(),
            &guard,
            &old_binding,
            &target,
            &staged_record,
            matches!(delivery, Delivery::CarrierFinalFalse),
        )
        .await;
        assert!(
            delivery_backend
                .get(&hot_layer_key(plan.carrier_layer_id))
                .await
                .unwrap()
                .is_none()
        );
        let current = delivery_backend
            .get(&packed_current_key(guard.workspace_id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            PackedLowerBindingRecord::decode(&current).unwrap(),
            old_binding
        );
        assert!(
            failure.publication.commit().await.is_err(),
            "ambiguous packet must not resubmit final CAS"
        );
        assert_eq!(delivery_backend.final_calls.load(Ordering::SeqCst), 1);
        drop(staged_record);
        meta.shutdown_session().await.unwrap();
        return;
    }
    let outcome = if matches!(delivery, Delivery::CancelWaiter) {
        let mut waiting = Box::pin(ready.commit());
        tokio::select! { result = &mut waiting => panic!("blocked response returned: {}", result.is_ok()), _ = delivery_backend.entered.notified() => {} }
        drop(waiting);
        assert_eq!(delivery_backend.final_calls.load(Ordering::SeqCst), 1);
        delivery_backend.release.notify_one();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            delivery_backend.done.notified(),
        )
        .await
        .unwrap();
        None
    } else {
        Some(match ready.commit().await {
            Ok(outcome) => outcome,
            Err(failure) => panic!("actual final publication failed: {}", failure.error),
        })
    };
    assert_eq!(delivery_backend.final_calls.load(Ordering::SeqCst), 1);
    carrier_basis_tests::assert_committed(
        delivery_backend.as_ref(),
        &target,
        &staged_record,
        source_root,
    )
    .await;
    if let Some(outcome) = &outcome {
        assert_eq!(outcome.binding.base_revision, plan.carrier_revision());
        assert_eq!(outcome.sealed_source.layer_id, guard.expected_head_layer_id);
        assert_eq!(outcome.sealed_source.root_hash, source_root);
        assert_ne!(outcome.sealed_source, outcome.binding.base_revision);
    }
    let new_guard = HeadGuard {
        expected_head_layer_id: target.head_layer_id,
        expected_head_epoch: target.head_epoch,
        ..guard.clone()
    };
    assert_eq!(
        store
            .load_packed_binding_record(new_guard.clone())
            .await
            .unwrap(),
        Some(target.clone())
    );
    let pair: [LayerRecord; 2] = store
        .load_layer_chain(target.head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(pair[0].state, LayerState::Writable);
    assert_eq!(pair[0].depth, 2);
    assert_eq!(pair[1].layer_id, plan.carrier_layer_id);
    assert_eq!(pair[1].state, LayerState::Sealed);
    assert_eq!(pair[1].parent_layer_id, None);
    assert_eq!(pair[1].depth, 1);
    assert_eq!(
        pair[1].delta_digest,
        Some(delta_digest(&CanonicalLayerDelta::default()).unwrap())
    );
    assert_eq!(
        pair[1].root_hash,
        Some(root_hash([0; 32], pair[1].delta_digest.unwrap()))
    );
    assert!(
        store
            .load_layer_delta(pair[1].layer_id)
            .await
            .unwrap()
            .inodes
            .is_empty(),
        "root inode comes from packed lower"
    );
    let sealed = store
        .load_layer(guard.expected_head_layer_id)
        .await
        .unwrap();
    assert_eq!(sealed.state, LayerState::Sealed);
    assert_eq!(sealed.delta_digest, Some(source_digest));
    assert_eq!(sealed.root_hash, Some(source_root));
    assert_eq!(sealed.sealed_version, Some(plan.source_sealed_version));
    assert_eq!(store.load_layer(old_base.layer_id).await.unwrap(), old_base);
    let history = delivery_backend
        .get(&packed_history_key(
            guard.workspace_id,
            old_binding.binding.binding_version,
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        PackedLowerBindingRecord::decode(&history).unwrap(),
        old_binding
    );
    let committed = delivery_backend
        .get(&journal_key(staged_record.journal_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        PackedJournalRecord::decode(&committed).unwrap().phase,
        PackedJournalPhase::Committed
    );
    assert!(
        delivery_backend
            .get(&active_key(staged_record.journal_id))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        active_count(&delivery_backend.get(ACTIVE_COUNT_KEY).await.unwrap()).unwrap(),
        0
    );
    assert!(
        delivery_backend
            .get(&open_v3_recovery_key(guard.workspace_id))
            .await
            .unwrap()
            .is_none()
    );
    let authenticated = AuthenticatedV3Snapshot::open(&client, &target.binding.manifest)
        .await
        .unwrap();
    let verified =
        crate::workspace_overlay::publish::binding::VerifiedPackedLower::from_authenticated_snapshot(
            &authenticated,
            &V3IndexReader::new(client.clone(), 0),
        )
        .await
        .unwrap();
    assert_eq!(
        verified.highest_inode(),
        inode.max(old_binding.highest_inode)
    );
    let candidate = PackedV3ReadonlyMeta::from_v3_budget(
        client.clone(),
        authenticated,
        4096,
        0,
        budget.clone(),
    )
    .unwrap();
    assert_eq!(
        candidate.stat(inode).await.unwrap().unwrap().size as usize,
        native_payload.len()
    );
    let prepared = candidate
        .prepare_unified_read(inode, 0, 0, native_payload.len() as u64)
        .await
        .unwrap()
        .unwrap();
    let mut actual_payload = vec![0; native_payload.len()];
    execute_unified_into(
        prepared.fetcher.as_ref(),
        0,
        &prepared.plan,
        &mut actual_payload,
    )
    .await
    .unwrap();
    assert_eq!(actual_payload, native_payload);
    drop(prepared);
    if let Some(outcome) = &outcome {
        assert_eq!(outcome.binding, target);
        assert_eq!(outcome.guard, new_guard);
        assert_eq!(outcome.sealed_source.root_hash, source_root);
        assert!(outcome.registry_report.unwrap().catalog_rows >= 2);
    }
    drop(candidate);
    drop(outcome);
    if matches!(delivery, Delivery::ForkAliases) {
        reader.shutdown().await.unwrap();
        fork_alias_tests::contract(fork_alias_tests::Fixture {
            store: &store,
            backend: &delivery_backend,
            client: client.clone(),
            budget: budget.clone(),
            connect: fork_context
                .as_ref()
                .expect("fork fixture connection factory")
                .1
                .clone(),
            parent_guard: new_guard.clone(),
            target: &target,
            committed: &staged_record,
            native_inode: inode,
            native_payload,
            original_lower_payload: &_lower_data,
        })
        .await;
    }
    if matches!(delivery, Delivery::HistoryRetirement) {
        // Release the original anchor reader without closing the shared mount
        // budget that the actual successor's retirement still needs.
        reader.shutdown().await.unwrap();
        history_retirement_tests::contract(history_retirement_tests::Fixture {
            store: &store,
            backend: &delivery_backend,
            client,
            objects: _objects.path(),
            budget,
            guard: new_guard,
            target: &target,
            anchor: &old_binding,
            committed: &staged_record,
        })
        .await;
    }
    meta.shutdown_session().await.unwrap();
}

async fn isolated<B: WorkspaceKvBackend>(backend: B, delivery: Delivery) {
    isolated_with_fork_context(backend, delivery, None).await;
}
async fn isolated_with_fork_context<B: WorkspaceKvBackend>(
    backend: B,
    delivery: Delivery,
    fork_context: Option<(Arc<V3MountBudget>, ForkConnect<B>)>,
) {
    let backend = Arc::new(backend);
    let worker = backend.clone();
    let result = tokio::spawn(async move { contract(worker, delivery, fork_context).await }).await;
    let entries = backend.scan_prefix(b"").await.unwrap();
    for batch in entries.chunks(32) {
        let checks = batch
            .iter()
            .map(|entry| KvCheck {
                key: entry.key.clone(),
                expected: Some(entry.value.clone()),
            })
            .collect::<Vec<_>>();
        let writes = batch
            .iter()
            .map(|entry| KvWrite::Delete {
                key: entry.key.clone(),
            })
            .collect::<Vec<_>>();
        assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
    }
    assert!(backend.scan_prefix(b"").await.unwrap().is_empty());
    backend.shutdown_metadata_backend().await.unwrap();
    match result {
        Ok(()) => {}
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => panic!("factory contract cancelled: {error}"),
    }
}
async fn redis(delivery: Delivery) {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    let namespace = format!("g12-native-factory-{}", Uuid::new_v4());
    if matches!(delivery, Delivery::ForkAliases) {
        let connect: ForkConnect<RedisWorkspaceBackend> = Arc::new(move |_budget| {
            let url = url.clone();
            let namespace = namespace.clone();
            Box::pin(async move { RedisWorkspaceBackend::connect(&url, &namespace).await })
        });
        let budget = V3MountBudget::defaults();
        let backend = connect(budget.clone()).await.unwrap();
        isolated_with_fork_context(backend, delivery, Some((budget, connect))).await;
        return;
    }
    isolated(
        RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap(),
        delivery,
    )
    .await;
}
async fn tikv(delivery: Delivery) {
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    let namespace = format!("g12-native-factory-{}", Uuid::new_v4());
    if matches!(delivery, Delivery::ForkAliases) {
        let connect: ForkConnect<TiKvWorkspaceBackend> = Arc::new(move |budget| {
            let endpoints = endpoints.clone();
            let namespace = namespace.clone();
            Box::pin(async move {
                TiKvWorkspaceBackend::connect_with_budget(endpoints, &namespace, budget).await
            })
        });
        let budget = V3MountBudget::defaults();
        let backend = connect(budget.clone()).await.unwrap();
        isolated_with_fork_context(backend, delivery, Some((budget, connect))).await;
        return;
    }
    isolated(
        TiKvWorkspaceBackend::connect(endpoints, &namespace)
            .await
            .unwrap(),
        delivery,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL; owns UUID namespace"]
async fn real_redis_native_factory_atomic_carrier_head_pwb_ppj_registry() {
    redis(Delivery::Normal).await
}
#[tokio::test]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS; owns UUID namespace"]
async fn real_tikv_native_factory_atomic_carrier_head_pwb_ppj_registry() {
    tikv(Delivery::Normal).await
}
#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL; owns UUID namespace"]
async fn real_redis_native_factory_lost_reply_exact_confirmation_without_reissue() {
    redis(Delivery::LostReply).await
}
#[tokio::test]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS; owns UUID namespace"]
async fn real_tikv_native_factory_lost_reply_exact_confirmation_without_reissue() {
    tikv(Delivery::LostReply).await
}
#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL; owns UUID namespace"]
async fn real_redis_native_factory_cancelled_waiter_keeps_owned_final_driver() {
    redis(Delivery::CancelWaiter).await
}
#[tokio::test]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS; owns UUID namespace"]
async fn real_tikv_native_factory_cancelled_waiter_keeps_owned_final_driver() {
    tikv(Delivery::CancelWaiter).await
}
#[tokio::test]
#[ignore = "requires BREWFS_TEST_REDIS_URL; owns UUID namespace"]
async fn real_redis_native_factory_unknown_before_cannot_resubmit_packet() {
    redis(Delivery::UnknownBefore).await
}
#[tokio::test]
#[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS; owns UUID namespace"]
async fn real_tikv_native_factory_unknown_before_cannot_resubmit_packet() {
    tikv(Delivery::UnknownBefore).await
}

#[path = "root_read_conflict_tests.rs"]
mod root_read_conflict_tests;

/// This probe deliberately has no ordinary bounded implementation. A default
/// wrapper fallback would fail, as it did with actual TiKV's ordinary cap32.
struct PublicationPacketOnlyBackend {
    calls: AtomicUsize,
}

#[async_trait]
impl WorkspaceKvBackend for PublicationPacketOnlyBackend {
    fn name(&self) -> &'static str {
        "publication-packet-forwarding-probe"
    }
    async fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        Ok(None)
    }
    async fn scan_prefix(&self, _prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        Ok(Vec::new())
    }
    async fn compare_and_swap(
        &self,
        _checks: &[KvCheck],
        _writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        Ok(false)
    }
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(100)
    }
    async fn get_publication_packet_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        crate::workspace_overlay::stores::kv_backend::validate_publication_packet_limits(
            keys, limits,
        )?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok((vec![None; keys.len()], 100))
    }
}

#[tokio::test]
async fn publication_packet_final_delivery_forwards_special_cap_and_preserves_read_observation() {
    let backend = Arc::new(PublicationPacketOnlyBackend {
        calls: AtomicUsize::new(0),
    });
    let delivery = FinalDelivery::new(backend.clone());
    delivery
        .mode
        .store(Delivery::LostReply as u8, Ordering::SeqCst);
    delivery.final_calls.store(1, Ordering::SeqCst);
    let keys: Vec<_> = (0..33)
        .map(|index| format!("authority/{index}").into_bytes())
        .collect();
    let limits = KvReadLimits {
        max_records: 33,
        max_key_bytes: 1024,
        max_value_bytes: 48 << 10,
        max_total_bytes: 2 << 20,
        max_response_bytes: 2 << 20,
        max_data_requests: 35,
    };
    let (values, now) = delivery
        .get_publication_packet_consistent_with_time_bounded(&keys, limits)
        .await
        .unwrap();
    assert_eq!((values, now), (vec![None; 33], 100));
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    {
        let observed = delivery.confirmation_reads.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].0, keys);
        assert_eq!(observed[0].1.max_data_requests, 35);
    }
    assert!(
        delivery
            .get_many_consistent_with_time_bounded(&keys, limits)
            .await
            .is_err()
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
}
