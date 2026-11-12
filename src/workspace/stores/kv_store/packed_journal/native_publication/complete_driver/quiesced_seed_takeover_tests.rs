//! Q-only takeover consumes a complete actual packed-journal absence proof.
//! The source fixture uses actual VFS drain/capture and the real first PNB API.

use super::*;
use crate::workspace::ids::LeaseId;
use crate::workspace::model::LeaseState;
use crate::workspace::packed_reader_lifecycle::PackedReaderSession;
use crate::workspace::stores::kv_store::packed_journal::tests::JournalMemoryBackend;
use crate::workspace::stores::kv_store::packed_native_freeze::{
    NativePrepareRecoveryRequest, PackedNativeQuiesceFence,
};
use std::collections::BTreeMap;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

const SEED_CLAIM_PREFIX: &[u8] = b"packed/native-recovery-claim/";
const NATIVE_DATA: &[u8] = b"actual Q-only original captured native bytes";

type Pause = (Arc<tokio::sync::Notify>, Arc<tokio::sync::Semaphore>);

#[derive(Default)]
struct SeedMemoryBackend {
    memory: JournalMemoryBackend,
    clock: AtomicI64,
    short_census_read: AtomicBool,
    census_scan_error: AtomicU8,
    pause_census: Mutex<Option<Pause>>,
    pause_takeover: Mutex<Option<Pause>>,
    pause_initial_prepare: Mutex<Option<Pause>>,
    recorded_first_pnb: Mutex<Option<Vec<KvWrite>>>,
    recorded_initial_prepare: Mutex<Option<CatalogPacket>>,
    initial_prepare_calls: AtomicUsize,
    lose_initial_prepare_reply: AtomicBool,
    deliver_first_pnb: Mutex<Option<Vec<KvWrite>>>,
    replace_epoch_on_takeover: AtomicBool,
    lose_takeover_reply: AtomicU8,
    takeover_attempts: AtomicUsize,
    takeover_commits: AtomicUsize,
    stale_takeovers: AtomicUsize,
}

impl SeedMemoryBackend {
    async fn census_scan(&self, prefix: &[u8]) -> Result<(), WorkspaceError> {
        if prefix != ACTIVE_PREFIX && prefix != JOURNAL_PREFIX {
            return Ok(());
        }
        if let Some((entered, release)) = self.pause_census.lock().await.take() {
            entered.notify_one();
            release.acquire().await.unwrap().forget();
        }
        match self.census_scan_error.swap(0, Ordering::SeqCst) {
            1 => Err(WorkspaceError::Backend(
                "injected incomplete actual PPJ census scan".into(),
            )),
            2 => Err(WorkspaceError::InvalidReadPlan(
                "actual PPJ census bounded row quota exceeded".into(),
            )),
            _ => Ok(()),
        }
    }

    async fn cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        self.cas_with_authentication_limits(checks, writes, lower, upper, None)
            .await
    }

    async fn cas_with_authentication_limits(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
        authentication_limits: Option<crate::workspace::stores::kv_backend::KvReadLimits>,
    ) -> Result<bool, WorkspaceError> {
        let initial_prepare = writes.iter().any(
            |write| matches!(write, KvWrite::Put { key, .. } if key.starts_with(b"packed/writer/")),
        ) && writes.iter().any(
            |write| matches!(write, KvWrite::Put { key, .. } if key.starts_with(OPEN_HANDLE_PREFIX)),
        ) && writes.iter().any(|write| match write {
            KvWrite::Put { key, value } if key.starts_with(HOT_JOURNAL_PREFIX) => {
                decode_open_value::<SealJournal>(value, 48 << 10).is_ok_and(|journal| {
                    journal.phase == SealPhase::Prepare
                        && *key == hot_journal_key(journal.workspace_id, journal.journal_id)
                })
            }
            _ => false,
        });
        if initial_prepare {
            self.initial_prepare_calls.fetch_add(1, Ordering::SeqCst);
            if let Some((entered, release)) = self.pause_initial_prepare.lock().await.take() {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            }
        }
        let takeover = writes.iter().any(
            |write| matches!(write, KvWrite::Put { key, .. } if key.starts_with(SEED_CLAIM_PREFIX)),
        );
        if takeover {
            self.takeover_attempts.fetch_add(1, Ordering::SeqCst);
            if let Some((entered, release)) = self.pause_takeover.lock().await.take() {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            }
        }
        let mut rows = self.memory.rows.lock().await;
        if takeover {
            if let Some(packet) = self.deliver_first_pnb.lock().await.take() {
                // Deliver the complete exact transaction captured from the
                // actual first-PNB call; no phase/source field is fabricated.
                for write in packet {
                    match write {
                        KvWrite::Put { key, value } => {
                            rows.insert(key, value);
                        }
                        KvWrite::Delete { key } => {
                            rows.remove(&key);
                        }
                    }
                }
            }
            if self.replace_epoch_on_takeover.load(Ordering::SeqCst) {
                let epoch: u64 = decode(rows.get(PACKED_ROOT_GENERATION_KEY).unwrap())?;
                rows.insert(PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&(epoch + 1))?);
            }
        }
        if let Some(limits) = authentication_limits {
            let mut authentication_total = 0usize;
            for check in checks {
                let value_bytes = rows.get(&check.key).map_or(0, Vec::len);
                authentication_total = authentication_total
                    .checked_add(check.key.len())
                    .and_then(|bytes| bytes.checked_add(value_bytes))
                    .ok_or_else(|| {
                        WorkspaceError::InvalidReadPlan(
                            "fixture authentication byte count overflow".into(),
                        )
                    })?;
                let response_bytes = check
                    .key
                    .len()
                    .checked_add(value_bytes)
                    .and_then(|bytes| bytes.checked_add(16))
                    .ok_or_else(|| {
                        WorkspaceError::InvalidReadPlan(
                            "fixture authentication response byte count overflow".into(),
                        )
                    })?;
                if value_bytes > limits.max_value_bytes
                    || authentication_total > limits.max_total_bytes
                    || response_bytes > limits.max_response_bytes
                {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "fixture authentication snapshot exceeds byte limits".into(),
                    ));
                }
            }
        }
        let now = self.clock.load(Ordering::SeqCst);
        if lower.is_some_and(|bound| now < bound)
            || upper.is_some_and(|bound| now >= bound)
            || checks
                .iter()
                .any(|check| rows.get(&check.key) != check.expected.as_ref())
        {
            if takeover {
                self.stale_takeovers.fetch_add(1, Ordering::SeqCst);
            }
            return Ok(false);
        }
        for write in writes {
            match write {
                KvWrite::Put { key, value } => {
                    rows.insert(key.clone(), value.clone());
                }
                KvWrite::Delete { key } => {
                    rows.remove(key);
                }
            }
        }
        if takeover {
            self.takeover_commits.fetch_add(1, Ordering::SeqCst);
        }
        if initial_prepare {
            *self.recorded_initial_prepare.lock().await = Some((checks.to_vec(), writes.to_vec()));
            if self
                .lose_initial_prepare_reply
                .swap(false, Ordering::SeqCst)
            {
                return Err(WorkspaceError::Backend(
                    "actual committed initial Prepare reply lost".into(),
                ));
            }
        }
        if writes.iter().any(|write| match write {
            KvWrite::Put { key, value } if key.starts_with(JOURNAL_PREFIX) => {
                PackedJournalRecord::decode(value).is_ok_and(|record| {
                    record.phase == PackedJournalPhase::Building && record.native_rebind.is_some()
                })
            }
            _ => false,
        }) {
            *self.recorded_first_pnb.lock().await = Some(writes.to_vec());
        }
        if takeover {
            let loss = self.lose_takeover_reply.swap(0, Ordering::SeqCst);
            if loss == 2 {
                let epoch: u64 = decode(rows.get(PACKED_ROOT_GENERATION_KEY).unwrap())?;
                rows.insert(PACKED_ROOT_GENERATION_KEY.to_vec(), encode(&(epoch + 1))?);
            }
            if loss != 0 {
                return Err(WorkspaceError::Backend(
                    "actual committed Q-only takeover reply lost".into(),
                ));
            }
        }
        Ok(true)
    }
}

#[async_trait]
impl WorkspaceKvBackend for SeedMemoryBackend {
    fn name(&self) -> &'static str {
        "quiesced-seed-takeover-memory-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        self.memory.get(key).await
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.memory.get_many_consistent(keys).await
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        Ok((
            self.memory.get_many_consistent(keys).await?,
            self.clock.load(Ordering::SeqCst),
        ))
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let (mut rows, _) = self
            .memory
            .get_many_consistent_with_time_bounded(keys, limits)
            .await?;
        if keys.len() == 3
            && keys[1] == JOURNAL_FEATURE_KEY
            && keys[2] == ACTIVE_COUNT_KEY
            && self.short_census_read.swap(false, Ordering::SeqCst)
        {
            rows.pop();
        }
        Ok((rows, self.clock.load(Ordering::SeqCst)))
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.memory.scan_prefix(prefix).await
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.census_scan(prefix).await?;
        self.memory
            .scan_prefix_with_byte_limits(prefix, limits)
            .await
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.census_scan(prefix).await?;
        self.memory
            .scan_prefix_page_with_byte_limits(prefix, after, limits)
            .await
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None, None).await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None, Some(deadline)).await
    }
    async fn compare_and_swap_in_time_window(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        lower: Option<i64>,
        upper: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, lower, upper).await
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        expires_at_ns: i64,
        limits: crate::workspace::stores::kv_backend::KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        crate::workspace::stores::kv_backend::validate_bounded_authentication_checks(
            checks, limits,
        )?;
        crate::workspace::stores::kv_backend::validate_cas_time_window(None, Some(expires_at_ns))?;
        self.cas_with_authentication_limits(checks, &[], None, Some(expires_at_ns), Some(limits))
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(self.clock.load(Ordering::SeqCst))
    }
}

struct Source {
    _objects: tempfile::TempDir,
    _scratch: tempfile::TempDir,
    backend: Arc<SeedMemoryBackend>,
    store: Arc<KvWorkspaceStore<SeedMemoryBackend>>,
    budget: Arc<SnapshotMountBudget>,
    old_guard: HeadGuard,
    native_journal: JournalId,
    artifact: Option<FrozenNativeArtifact<SeedMemoryBackend, InMemoryBlockStore>>,
    reader: Option<Arc<dyn PackedReaderSession>>,
    original_q: Vec<u8>,
    owner: String,
    new_lease: LeaseId,
}

impl Source {
    async fn new() -> Self {
        Self::with_initial_prepare_reply_loss(false).await
    }

    async fn with_initial_prepare_reply_loss(lose_reply: bool) -> Self {
        let (objects, client, snapshot, lower_proof, _) = packed().await;
        let scratch = tempfile::tempdir().unwrap();
        let backend = Arc::new(SeedMemoryBackend::default());
        backend.clock.store(1_000_000_000, Ordering::SeqCst);
        let budget = SnapshotMountBudget::defaults();
        let store = Arc::new(
            KvWorkspaceStore::from_arc(backend.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        let install = request(store.as_ref(), lower_proof).await;
        let binding = store
            .install_packed_lower_binding(install.clone())
            .await
            .unwrap();
        let old_guard = HeadGuard {
            expected_head_epoch: binding.head_epoch,
            ..install.guard
        };
        let reader = store
            .clone()
            .open_packed_reader_session(
                old_guard.clone(),
                budget.clone(),
                PackedReaderLeaseOptions::default(),
            )
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&reader.mount_budget(), &budget));
        let lower = Arc::new(
            PackedReadonlyMeta::from_snapshot_budget(client, snapshot, 4096, 0, budget.clone())
                .unwrap(),
        );
        let upper = Arc::new(InMemoryBlockStore::new());
        let layout = ChunkLayout {
            chunk_size: 4096,
            block_size: 4096,
        };
        let meta = Arc::new(
            WorkspaceMetaLayer::with_chunk_size(
                store.clone(),
                ViewContext {
                    workspace_id: old_guard.workspace_id,
                    head_layer_id: old_guard.expected_head_layer_id,
                    head_epoch: old_guard.expected_head_epoch,
                    lease_id: old_guard.lease_id,
                    holder_generation: old_guard.holder_generation,
                },
                4096,
            )
            .with_packed_lower(
                binding.binding.clone(),
                lower,
                Arc::new(PinnedCatalogPackedBindingAuthority {
                    store: store.clone(),
                    reader: reader.clone(),
                }),
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
        let inode = vfs.create_file("/q-only-native").await.unwrap();
        let handle = vfs
            .open(
                inode,
                vfs.stat("/q-only-native").await.unwrap(),
                true,
                true,
                false,
            )
            .await
            .unwrap();
        assert_eq!(
            vfs.write(handle, 0, NATIVE_DATA).await.unwrap(),
            NATIVE_DATA.len()
        );
        vfs.flush(handle).await.unwrap();
        vfs.close(handle).await.unwrap();
        let local = vfs.quiesce_packed_vfs().await.unwrap();
        let layers: [LayerRecord; 2] = store
            .load_layer_chain(old_guard.expected_head_layer_id)
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let native_journal = JournalId::new();
        backend
            .lose_initial_prepare_reply
            .store(lose_reply, Ordering::SeqCst);
        let native: Arc<PackedNativeQuiesceFence<SeedMemoryBackend>> = Arc::new(
            store
                .clone()
                .begin_packed_native_quiesce(
                    old_guard.clone(),
                    layers,
                    native_journal,
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
        drop(vfs);
        drop(meta);
        let source = Self {
            _objects: objects,
            _scratch: scratch,
            backend,
            store,
            budget,
            old_guard,
            native_journal,
            artifact: Some(artifact),
            reader: Some(reader),
            original_q,
            owner: format!("q-only-owner-{}", Uuid::new_v4()),
            new_lease: LeaseId::new(),
        };
        source.assert_original_q_no_claim().await;
        source
    }

    async fn assert_original_q_no_claim(&self) {
        let control = test_topology_from_rows(&*self.backend.memory.rows.lock().await);
        assert_eq!(
            control.journals.get(&self.native_journal).unwrap().phase,
            SealPhase::Quiesced
        );
        assert!(self.backend.get(&self.claim_key()).await.unwrap().is_none());
    }

    fn claim_key(&self) -> Vec<u8> {
        [
            SEED_CLAIM_PREFIX,
            self.native_journal.to_string().as_bytes(),
        ]
        .concat()
    }

    async fn first_pnb(&self) -> PackedJournalRecord {
        let record = self
            .store
            .begin_native_packed_journal(self.artifact.as_ref().unwrap())
            .await
            .unwrap();
        assert_eq!(record.phase, PackedJournalPhase::Building);
        assert_eq!(
            record.native_rebind.as_ref().unwrap().native_journal_id,
            self.native_journal
        );
        self.assert_original_q_no_claim().await;
        let record = (*record).clone();
        assert_eq!(
            self.backend
                .get(&journal_key(record.journal_id))
                .await
                .unwrap(),
            Some(record.encode().unwrap())
        );
        assert_eq!(
            self.backend
                .get(&active_key(record.journal_id))
                .await
                .unwrap(),
            Some(record.encode().unwrap())
        );
        record
    }

    async fn open_recovery(&mut self, expire: bool) {
        let original_open: SnapshotOpenRecord = decode_open_value(
            &self
                .backend
                .get(&open_snapshot_key(self.old_guard.workspace_id))
                .await
                .unwrap()
                .unwrap(),
            OPEN_RECORD_MAX_BYTES,
        )
        .unwrap();
        assert_eq!(original_open.state, SnapshotOpenState::Ready);
        assert!(!original_open.recovery_required);
        // Drop the local source and reader as an abandoned process would. Drop
        // does not claim asynchronous release or remove its durable rows.
        drop(self.artifact.take());
        drop(self.reader.take());
        if expire {
            let lease: SnapshotLease = decode(
                &self
                    .backend
                    .get(&hot_lease_key(
                        self.old_guard.workspace_id,
                        self.old_guard.lease_id,
                    ))
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            self.backend
                .clock
                .store(lease.expires_at_ns, Ordering::SeqCst);
            assert!(original_open.expires_at_ns <= lease.expires_at_ns);
        }
        // Resume the actual persisted administrative owner. A public open may
        // advance that owner into recovery; it cannot replace it with a new ID.
        self.owner = original_open.owner_id.clone();
        let open = self
            .store
            .open_workspace_snapshot(
                self.old_guard.workspace_id,
                self.owner.clone(),
                Duration::from_secs(300),
            )
            .await
            .unwrap();
        assert_eq!(open.state, SnapshotOpenState::Recovering);
        assert!(open.recovery_required);
        assert_eq!(
            open.generation,
            original_open.generation + u64::from(expire)
        );
        self.assert_original_q_no_claim().await;
    }

    fn request(&self, new: bool) -> NativePrepareRecoveryRequest {
        NativePrepareRecoveryRequest {
            journal_id: self.native_journal,
            owner_id: self.owner.clone(),
            new_lease_id: new.then_some(self.new_lease),
            ttl_ns: 300_000_000_000,
        }
    }

    async fn assert_no_takeover(&self) {
        self.assert_original_q_no_claim().await;
        assert!(
            self.backend
                .get(&hot_lease_key(self.old_guard.workspace_id, self.new_lease))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(self.backend.takeover_commits.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn quiesced_seed_takeover_expired_empty_q_installs_exact_new_owner() {
    let mut source = Source::new().await;
    source.open_recovery(true).await;
    let fence = source
        .store
        .recover_packed_native_prepare(source.request(true), source.budget.clone())
        .await
        .unwrap();
    assert_eq!(fence.mapping().old_guard(), &source.old_guard);
    assert_eq!(fence.mapping().journal_id(), source.native_journal);
    assert_eq!(fence.canonical_receipt_bytes(), source.original_q);
    assert_eq!(fence.source_guard().lease_id, source.new_lease);
    assert_eq!(
        fence.source_guard().holder_generation,
        source.old_guard.holder_generation + 1
    );
    let old: SnapshotLease = decode(
        &source
            .backend
            .get(&hot_lease_key(
                source.old_guard.workspace_id,
                source.old_guard.lease_id,
            ))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let new: SnapshotLease = decode(
        &source
            .backend
            .get(&hot_lease_key(
                source.old_guard.workspace_id,
                source.new_lease,
            ))
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(old.state, LeaseState::Expired);
    assert_eq!(new.state, LeaseState::Active);
    assert!(
        source
            .backend
            .get(&source.claim_key())
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(source.backend.takeover_commits.load(Ordering::SeqCst), 1);
    fence.validate().await.unwrap();
}

#[tokio::test]
async fn quiesced_seed_takeover_lost_reply_confirms_full_census_without_replay() {
    for loss in [1, 2] {
        let mut source = Source::new().await;
        source.open_recovery(true).await;
        source
            .backend
            .lose_takeover_reply
            .store(loss, Ordering::SeqCst);
        let result = source
            .store
            .recover_packed_native_prepare(source.request(true), source.budget.clone())
            .await;
        if loss == 1 {
            let fence = result.unwrap();
            assert_eq!(fence.source_guard().lease_id, source.new_lease);
            assert_eq!(fence.canonical_receipt_bytes(), source.original_q);
            fence.validate().await.unwrap();
        } else {
            assert!(matches!(result, Err(WorkspaceError::Backend(_))));
        }
        assert_eq!(source.backend.takeover_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(source.backend.takeover_commits.load(Ordering::SeqCst), 1);
        assert!(
            source
                .backend
                .get(&source.claim_key())
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            source
                .backend
                .get(&hot_lease_key(
                    source.old_guard.workspace_id,
                    source.new_lease
                ))
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test]
async fn quiesced_seed_takeover_live_original_q_cannot_issue_claimless_source() {
    let source = Source::new().await;
    let before = source.backend.memory.rows.lock().await.clone();
    // Prepare now persisted the original live Ready open. A different owner
    // cannot acquire a recovery open while that original authority is live.
    assert!(matches!(
        source
            .store
            .open_workspace_snapshot(
                source.old_guard.workspace_id,
                source.owner.clone(),
                Duration::from_secs(300),
            )
            .await,
        Err(WorkspaceError::Busy)
    ));
    assert_eq!(*source.backend.memory.rows.lock().await, before);
    for new in [false, true] {
        assert!(matches!(
            source
                .store
                .recover_packed_native_prepare(source.request(new), source.budget.clone())
                .await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(*source.backend.memory.rows.lock().await, before);
        source.assert_no_takeover().await;
    }
}

#[tokio::test]
async fn quiesced_seed_takeover_actual_original_building_pnb_keeps_q_and_all_rows() {
    let mut source = Source::new().await;
    let record = source.first_pnb().await;
    source.open_recovery(true).await;
    let before = source.backend.memory.rows.lock().await.clone();
    assert!(
        source
            .store
            .recover_packed_native_prepare(source.request(true), source.budget.clone())
            .await
            .is_err()
    );
    assert_eq!(*source.backend.memory.rows.lock().await, before);
    source.assert_no_takeover().await;
    assert_eq!(
        source
            .backend
            .get(&journal_key(record.journal_id))
            .await
            .unwrap(),
        Some(record.encode().unwrap())
    );
}

#[tokio::test]
async fn quiesced_seed_takeover_missing_sentinels_main_or_active_remnants_are_not_empty() {
    for keep in 0..3 {
        let mut source = Source::new().await;
        let record = source.first_pnb().await;
        source.open_recovery(true).await;
        let mut rows = source.backend.memory.rows.lock().await;
        rows.remove(JOURNAL_FEATURE_KEY);
        rows.remove(ACTIVE_COUNT_KEY);
        if keep == 0 {
            rows.remove(&active_key(record.journal_id));
        }
        if keep == 1 {
            rows.remove(&journal_key(record.journal_id));
        }
        let before = rows.clone();
        drop(rows);
        assert!(
            source
                .store
                .recover_packed_native_prepare(source.request(true), source.budget.clone())
                .await
                .is_err()
        );
        assert_eq!(*source.backend.memory.rows.lock().await, before);
        source.assert_no_takeover().await;
    }
}

#[tokio::test]
async fn quiesced_seed_takeover_short_or_failed_or_quota_census_cannot_authorize_zero() {
    for failure in 0..3 {
        let mut source = Source::new().await;
        source.open_recovery(true).await;
        match failure {
            0 => source
                .backend
                .short_census_read
                .store(true, Ordering::SeqCst),
            1 => source.backend.census_scan_error.store(1, Ordering::SeqCst),
            2 => source.backend.census_scan_error.store(2, Ordering::SeqCst),
            _ => unreachable!(),
        }
        let before = source.backend.memory.rows.lock().await.clone();
        assert!(
            source
                .store
                .recover_packed_native_prepare(source.request(true), source.budget.clone())
                .await
                .is_err()
        );
        assert_eq!(*source.backend.memory.rows.lock().await, before);
        source.assert_no_takeover().await;
    }
}

#[tokio::test]
async fn quiesced_seed_takeover_closed_budget_during_census_preserves_all_rows() {
    let mut source = Source::new().await;
    source.open_recovery(true).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    *source.backend.pause_census.lock().await = Some((entered.clone(), release.clone()));
    let before = source.backend.memory.rows.lock().await.clone();
    let store = source.store.clone();
    let budget = source.budget.clone();
    let request = source.request(true);
    let task =
        tokio::spawn(async move { store.recover_packed_native_prepare(request, budget).await });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    source.budget.close();
    release.add_permits(1);
    assert!(task.await.unwrap().is_err());
    assert_eq!(*source.backend.memory.rows.lock().await, before);
    source.assert_no_takeover().await;
}

#[tokio::test]
async fn quiesced_seed_takeover_concurrent_epoch_replacement_fences_every_old_attempt() {
    let mut source = Source::new().await;
    source.open_recovery(true).await;
    let mut before = source.backend.memory.rows.lock().await.clone();
    source
        .backend
        .replace_epoch_on_takeover
        .store(true, Ordering::SeqCst);
    assert!(
        source
            .store
            .recover_packed_native_prepare(source.request(true), source.budget.clone())
            .await
            .is_err()
    );
    let mut after = source.backend.memory.rows.lock().await.clone();
    assert_ne!(
        before.remove(PACKED_ROOT_GENERATION_KEY),
        after.remove(PACKED_ROOT_GENERATION_KEY)
    );
    assert_eq!(after, before);
    assert!(source.backend.stale_takeovers.load(Ordering::SeqCst) > 0);
    source.assert_no_takeover().await;
}

#[tokio::test]
async fn quiesced_seed_takeover_first_pnb_delivery_after_empty_census_fences_takeover() {
    let mut source = Source::new().await;
    let prebirth = source.backend.memory.rows.lock().await.clone();
    let record = source.first_pnb().await;
    let packet = source
        .backend
        .recorded_first_pnb
        .lock()
        .await
        .clone()
        .unwrap();
    // Restore the transaction's exact Memory before-image so the scheduled
    // recovery sees no PNB until the recorded actual packet is delivered.
    *source.backend.memory.rows.lock().await = prebirth;
    source.open_recovery(true).await;
    *source.backend.deliver_first_pnb.lock().await = Some(packet);
    assert!(
        source
            .store
            .recover_packed_native_prepare(source.request(true), source.budget.clone())
            .await
            .is_err()
    );
    assert_eq!(source.backend.stale_takeovers.load(Ordering::SeqCst), 1);
    assert_eq!(source.backend.takeover_attempts.load(Ordering::SeqCst), 1);
    source.assert_no_takeover().await;
    assert_eq!(
        source
            .backend
            .get(&journal_key(record.journal_id))
            .await
            .unwrap(),
        Some(record.encode().unwrap())
    );
    assert_eq!(
        source
            .backend
            .get(&active_key(record.journal_id))
            .await
            .unwrap(),
        Some(record.encode().unwrap())
    );
}

#[tokio::test]
async fn quiesced_seed_takeover_sentinel_present_main_only_cannot_authorize_takeover() {
    let mut source = Source::new().await;
    let record = source.first_pnb().await;
    source.open_recovery(true).await;
    let raw = record.encode().unwrap();
    let mut rows = source.backend.memory.rows.lock().await;
    assert_eq!(rows.get(JOURNAL_FEATURE_KEY).unwrap().as_slice(), b"PPJ3");
    assert_eq!(rows.get(&journal_key(record.journal_id)), Some(&raw));
    assert_eq!(
        rows.remove(&active_key(record.journal_id)),
        Some(raw.clone())
    );
    rows.insert(ACTIVE_COUNT_KEY.to_vec(), 0_u64.to_le_bytes().to_vec());
    let before = rows.clone();
    drop(rows);
    assert!(
        source
            .store
            .recover_packed_native_prepare(source.request(true), source.budget.clone())
            .await
            .is_err()
    );
    assert_eq!(*source.backend.memory.rows.lock().await, before);
    source.assert_no_takeover().await;
    assert_eq!(
        source
            .backend
            .get(&journal_key(record.journal_id))
            .await
            .unwrap(),
        Some(raw)
    );
}

#[tokio::test]
async fn quiesced_seed_initial_prepare_claim_is_atomic_and_lost_reply_is_confirmed_without_replay()
{
    use crate::workspace::stores::kv_store::packed_writer_authority::{
        PackedWriterAuthority, PackedWriterOwner, packed_writer_key,
    };
    for lose_reply in [false, true] {
        let source = Source::with_initial_prepare_reply_loss(lose_reply).await;
        assert_eq!(
            source.backend.initial_prepare_calls.load(Ordering::SeqCst),
            1
        );
        let (checks, writes) = source
            .backend
            .recorded_initial_prepare
            .lock()
            .await
            .clone()
            .unwrap();
        let workspace = source.old_guard.workspace_id;
        let writer_key = packed_writer_key(workspace);
        let open_key = open_snapshot_key(workspace);
        let old_writer = PackedWriterAuthority::decode(
            checks
                .iter()
                .find(|check| check.key == writer_key)
                .unwrap()
                .expected
                .as_deref()
                .unwrap(),
            workspace,
        )
        .unwrap();
        assert!(
            matches!(old_writer.owner, Some(PackedWriterOwner::InitialSource {
            lease_id, holder_generation,
        }) if lease_id == source.old_guard.lease_id
            && holder_generation == source.old_guard.holder_generation)
        );
        assert!(
            checks
                .iter()
                .find(|check| check.key == open_key)
                .unwrap()
                .expected
                .is_none()
        );
        let written = |key: &[u8]| {
            writes
                .iter()
                .find_map(|write| match write {
                    KvWrite::Put { key: actual, value } if actual.as_slice() == key => {
                        Some(value.as_slice())
                    }
                    _ => None,
                })
                .unwrap()
        };
        let open: SnapshotOpenRecord = decode(written(&open_key)).unwrap();
        let writer = PackedWriterAuthority::decode(written(&writer_key), workspace).unwrap();
        assert_eq!(writer.incarnation, old_writer.incarnation + 1);
        assert_eq!(open.state, SnapshotOpenState::Ready);
        assert!(!open.recovery_required);
        assert!(
            matches!(writer.owner, Some(PackedWriterOwner::Administrative {
            lease_id, holder_generation, ref open_owner, open_generation, recovering: false,
        }) if lease_id == source.old_guard.lease_id
            && holder_generation == source.old_guard.holder_generation
            && open_owner == &open.owner_id
            && open_generation == open.generation)
        );
        let mut successor_rows = checks
            .iter()
            .filter_map(|check| {
                check
                    .expected
                    .as_ref()
                    .map(|raw| (check.key.clone(), raw.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        for write in &writes {
            match write {
                KvWrite::Put { key, value } => {
                    successor_rows.insert(key.clone(), value.clone());
                }
                KvWrite::Delete { key } => {
                    successor_rows.remove(key);
                }
            }
        }
        assert!(
            checks
                .iter()
                .any(|check| { check.key.as_slice() == CONTROL_KEY && check.expected.is_some() })
        );
        assert!(writes.iter().all(|write| match write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => key.as_slice() != CONTROL_KEY,
        }));
        let control = test_topology_from_rows(&successor_rows);
        assert_eq!(
            control.journals.get(&source.native_journal).unwrap().phase,
            SealPhase::Prepare
        );
        assert_eq!(
            control.workspaces.get(&workspace).unwrap().state,
            WorkspaceState::Sealing
        );
        assert_eq!(
            control.workspaces.get(&workspace).unwrap().active_lease,
            Some(source.old_guard.lease_id)
        );
        assert_eq!(
            open.expires_at_ns,
            control
                .leases
                .get(&source.old_guard.lease_id)
                .unwrap()
                .expires_at_ns
        );
        source.assert_original_q_no_claim().await;
    }
}

#[tokio::test]
async fn quiesced_seed_initial_prepare_rejects_concurrent_open_writer_and_expiry_without_partial_commit()
 {
    use crate::workspace::stores::kv_store::packed_writer_authority::{
        PackedWriterAuthority, PackedWriterOwner, packed_writer_key,
    };
    for conflict in 0..3 {
        let (_objects, _client, _snapshot, proof, _) = packed().await;
        let backend = Arc::new(SeedMemoryBackend::default());
        backend.clock.store(1_000_000_000, Ordering::SeqCst);
        let budget = SnapshotMountBudget::defaults();
        let store = Arc::new(
            KvWorkspaceStore::from_arc(backend.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        let install = request(store.as_ref(), proof).await;
        let binding = store
            .install_packed_lower_binding(install.clone())
            .await
            .unwrap();
        let guard = HeadGuard {
            expected_head_epoch: binding.head_epoch,
            ..install.guard
        };
        let layers = store
            .load_layer_chain(guard.expected_head_layer_id)
            .await
            .unwrap()
            .try_into()
            .unwrap();
        let journal = JournalId::new();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        *backend.pause_initial_prepare.lock().await = Some((entered.clone(), release.clone()));
        let pending = store.clone().begin_packed_native_quiesce(
            guard.clone(),
            layers,
            journal,
            LayerId::new(),
            budget.clone(),
        );
        let caller = tokio::spawn(pending);
        entered.notified().await;
        let mut rows = backend.memory.rows.lock().await;
        let lease: SnapshotLease = decode(
            rows.get(&hot_lease_key(guard.workspace_id, guard.lease_id))
                .unwrap(),
        )
        .unwrap();
        match conflict {
            0 => {
                let foreign_open = SnapshotOpenRecord {
                    workspace_id: guard.workspace_id,
                    owner_id: "actual-concurrent-open-owner".into(),
                    generation: 1,
                    expires_at_ns: lease.expires_at_ns,
                    state: SnapshotOpenState::Ready,
                    recovery_required: false,
                };
                rows.insert(
                    open_snapshot_key(guard.workspace_id),
                    encode(&foreign_open).unwrap(),
                );
            }
            1 => {
                let key = packed_writer_key(guard.workspace_id);
                let mut writer =
                    PackedWriterAuthority::decode(rows.get(&key).unwrap(), guard.workspace_id)
                        .unwrap();
                writer.owner = Some(PackedWriterOwner::InitialSource {
                    lease_id: LeaseId::new(),
                    holder_generation: guard.holder_generation + 1,
                });
                rows.insert(key, writer.encode().unwrap());
            }
            _ => backend.clock.store(lease.expires_at_ns, Ordering::SeqCst),
        }
        let exact_after_conflict = rows.clone();
        drop(rows);
        release.add_permits(1);
        assert!(matches!(caller.await.unwrap(), Err(WorkspaceError::Fenced)));
        assert_eq!(*backend.memory.rows.lock().await, exact_after_conflict);
        assert_eq!(backend.initial_prepare_calls.load(Ordering::SeqCst), 1);
        assert!(backend.recorded_initial_prepare.lock().await.is_none());
        let control = test_topology_from_rows(&*backend.memory.rows.lock().await);
        assert!(!control.journals.contains_key(&journal));
        assert_eq!(
            control.workspaces.get(&guard.workspace_id).unwrap().state,
            WorkspaceState::Active
        );
        assert_eq!(budget.state().used, [0; 8]);
    }
}
