//! Journal persistence contracts on the shared Redis/TiKV substrate.
//! Test-only seals exercise atomicity; they are not full graph evidence.

use super::*;
use crate::workspace_overlay::packed_v3::wire005::read_v3_page;
use crate::workspace_overlay::stores::binding_tests::{packed, packed_with_snapshot_id, request};
use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
use crate::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

#[derive(Clone)]
pub(super) struct JournalMemoryBackend {
    pub(super) rows: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    now: Arc<AtomicI64>,
    pub(super) lose_reply: Arc<AtomicBool>,
    pub(super) lose_nth_write_reply: Arc<AtomicI64>,
    expire_at_cas: Arc<AtomicBool>,
    bounded_calls: Arc<AtomicI64>,
    pub(super) page_size: Arc<AtomicI64>,
    pub(super) page_calls: Arc<AtomicI64>,
}
impl Default for JournalMemoryBackend {
    fn default() -> Self {
        Self {
            rows: Default::default(),
            now: Arc::new(AtomicI64::new(1)),
            lose_reply: Default::default(),
            lose_nth_write_reply: Default::default(),
            expire_at_cas: Default::default(),
            bounded_calls: Default::default(),
            page_size: Default::default(),
            page_calls: Default::default(),
        }
    }
}
impl JournalMemoryBackend {
    pub(super) fn at_time(now: i64) -> Self {
        Self {
            now: Arc::new(AtomicI64::new(now)),
            ..Self::default()
        }
    }

    async fn cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: Option<i64>,
    ) -> Result<bool, WorkspaceError> {
        self.cas_with_authentication_limits(checks, writes, deadline, None)
            .await
    }

    async fn cas_with_authentication_limits(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: Option<i64>,
        authentication_limits: Option<crate::workspace_overlay::stores::kv_backend::KvReadLimits>,
    ) -> Result<bool, WorkspaceError> {
        let mut rows = self.rows.lock().await;
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
        if checks
            .iter()
            .any(|check| rows.get(&check.key) != check.expected.as_ref())
        {
            return Ok(false);
        }
        if let Some(deadline) = deadline {
            if self.expire_at_cas.swap(false, Ordering::SeqCst) {
                self.now.store(deadline, Ordering::SeqCst);
            }
            if self.now.load(Ordering::SeqCst) >= deadline {
                return Err(WorkspaceError::Fenced);
            }
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
        let nth = if writes.is_empty() {
            0
        } else {
            self.lose_nth_write_reply
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    (remaining > 0).then_some(remaining - 1)
                })
                .unwrap_or(0)
        };
        if !writes.is_empty() && (self.lose_reply.swap(false, Ordering::SeqCst) || nth == 1) {
            return Err(WorkspaceError::Backend(
                "injected committed journal response loss".into(),
            ));
        }
        Ok(true)
    }
}
#[async_trait]
impl WorkspaceKvBackend for JournalMemoryBackend {
    fn name(&self) -> &'static str {
        "packed-journal-memory-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        Ok(self.rows.lock().await.get(key).cloned())
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        let rows = self.rows.lock().await;
        Ok(keys.iter().map(|key| rows.get(key).cloned()).collect())
    }
    async fn get_many_consistent_with_time(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let rows = self.rows.lock().await;
        Ok((
            keys.iter().map(|key| rows.get(key).cloned()).collect(),
            self.now.load(Ordering::SeqCst),
        ))
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        Ok(self
            .rows
            .lock()
            .await
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect())
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        limits.validate_keys(keys)?;
        self.bounded_calls.fetch_add(1, Ordering::SeqCst);
        let rows = self.rows.lock().await;
        let mut total = 0usize;
        for key in keys {
            let length = rows.get(key).map_or(0, Vec::len);
            total = total
                .checked_add(key.len())
                .and_then(|n| n.checked_add(length))
                .ok_or_else(|| WorkspaceError::InvalidReadPlan("memory KV byte overflow".into()))?;
            if length > limits.max_value_bytes
                || total > limits.max_total_bytes
                || total.saturating_add(256 + keys.len() * 16) > limits.max_response_bytes
            {
                return Err(WorkspaceError::InvalidReadPlan(
                    "memory KV bounded value rejected before clone".into(),
                ));
            }
        }
        Ok((
            keys.iter().map(|key| rows.get(key).cloned()).collect(),
            self.now.load(Ordering::SeqCst),
        ))
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate()?;
        self.bounded_calls.fetch_add(1, Ordering::SeqCst);
        let rows = self.rows.lock().await;
        let mut total = 0usize;
        for (key, value) in rows
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .take(limits.max_records)
        {
            total = total
                .checked_add(key.len())
                .and_then(|n| n.checked_add(value.len()))
                .ok_or_else(|| WorkspaceError::InvalidReadPlan("memory KV byte overflow".into()))?;
            if key.len() > limits.max_key_bytes
                || value.len() > limits.max_value_bytes
                || total > limits.max_total_bytes
                || total.saturating_add(256 + limits.max_records * 16) > limits.max_response_bytes
            {
                return Err(WorkspaceError::InvalidReadPlan(
                    "memory KV bounded scan rejected before clone".into(),
                ));
            }
        }
        Ok(rows
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .take(limits.max_records)
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect())
    }
    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        if limit == 0 {
            return Err(WorkspaceError::InvalidReadPlan("zero scan limit".into()));
        }
        Ok(self
            .rows
            .lock()
            .await
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .take(limit)
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect())
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after_key_exclusive: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate_scan_page(prefix, after_key_exclusive)?;
        self.page_calls.fetch_add(1, Ordering::SeqCst);
        let rows = self.rows.lock().await;
        let configured = self.page_size.load(Ordering::SeqCst);
        let cap = if configured > 0 {
            limits.max_records.min(configured as usize)
        } else {
            limits.max_records
        };
        let selected = rows
            .iter()
            .filter(|(key, _)| {
                key.starts_with(prefix)
                    && after_key_exclusive.is_none_or(|after| key.as_slice() > after)
            })
            .take(cap);
        let mut total = 0usize;
        for (key, value) in selected.clone() {
            total = total
                .checked_add(key.len())
                .and_then(|n| n.checked_add(value.len()))
                .ok_or_else(|| WorkspaceError::InvalidReadPlan("memory page overflow".into()))?;
            if key.len() > limits.max_key_bytes
                || value.len() > limits.max_value_bytes
                || total > limits.max_total_bytes
                || total.saturating_add(256 + limits.max_records * 16) > limits.max_response_bytes
            {
                return Err(WorkspaceError::InvalidReadPlan(
                    "memory bounded page rejected before clone".into(),
                ));
            }
        }
        Ok(selected
            .map(|(key, value)| KvEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect())
    }
    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, None).await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        self.cas(checks, writes, Some(deadline)).await
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
        self.cas_with_authentication_limits(checks, &[], Some(expires_at_ns), Some(limits))
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(self.now.load(Ordering::SeqCst))
    }
}

fn source_view() -> PackedSourceView {
    PackedSourceView {
        snapshot_backed: false,
        effective_view_digest: [11; 32],
        frozen_view_token: [12; 32],
        build_provenance_digest: [13; 32],
        build_owner: "journal-contract-build".into(),
        staging_id: Uuid::new_v4(),
        staging_prefix: format!("journal-contract/{}", Uuid::new_v4().simple()),
    }
}

#[tokio::test]
async fn packed_journal_gc_probe_admits_before_read_and_retains_no_feature_checks() {
    let backend = Arc::new(JournalMemoryBackend::default());
    let store = KvWorkspaceStore::from_arc(backend.clone());
    assert!(matches!(
        store.scan_packed_journal_layer_roots().await,
        Err(WorkspaceError::UnsupportedCapability(
            "packed journal GC memory budget"
        ))
    ));
    assert_eq!(backend.bounded_calls.load(Ordering::SeqCst), 0);
    let budget = V3MountBudget::defaults();
    store
        .configure_packed_reader_pin_budget(budget.clone())
        .unwrap();
    let before = budget.state().used[V3BudgetPool::Metadata as usize];
    let (roots, checks, owner) = store.scan_packed_journal_layer_roots().await.unwrap();
    assert!(roots.is_empty());
    assert_eq!(checks.len(), 3);
    assert!(budget.state().used[V3BudgetPool::Metadata as usize] > before);
    assert!(backend.compare_and_swap(&checks, &[]).await.unwrap());
    drop((checks, owner));
    assert_eq!(budget.state().used[V3BudgetPool::Metadata as usize], before);
}

#[tokio::test]
async fn packed_journal_bounded_reopen_and_probe_reject_oversized_rows() {
    let backend = Arc::new(JournalMemoryBackend::default());
    let budget = V3MountBudget::defaults();
    let store =
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    let view = source_view();
    let id = JournalId::new();
    backend
        .rows
        .lock()
        .await
        .insert(journal_key(id), vec![0; RECORD_LIMIT + 1]);
    assert!(matches!(
        store.reopen_packed_journal(id, &view, &budget).await,
        Err(WorkspaceError::InvalidReadPlan(_))
    ));
    backend
        .rows
        .lock()
        .await
        .insert(JOURNAL_FEATURE_KEY.to_vec(), vec![0; 513]);
    assert!(matches!(
        store.scan_packed_journal_layer_roots().await,
        Err(WorkspaceError::InvalidReadPlan(_))
    ));
    assert_eq!(budget.state().used[V3BudgetPool::Metadata as usize], 0);
}

#[cfg(target_os = "linux")]
fn imported_objects(directory: &std::path::Path) -> Vec<V3ObjectRef> {
    let kinds = [
        V3ObjectKind::Manifest,
        V3ObjectKind::GroupContainer,
        V3ObjectKind::ColdAttributes,
        V3ObjectKind::FrameDirectory,
        V3ObjectKind::LargeData,
        V3ObjectKind::GroupIndex,
        V3ObjectKind::InodeIndex,
        V3ObjectKind::ReverseIndex,
        V3ObjectKind::ContainerIndex,
        V3ObjectKind::FrameIndex,
        V3ObjectKind::ColdIndex,
        V3ObjectKind::LargeIndex,
        V3ObjectKind::SourceStatsIndex,
    ];
    let mut pending = vec![directory.to_owned()];
    let mut objects = Vec::new();
    while let Some(parent) = pending.pop() {
        for entry in std::fs::read_dir(parent).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
                continue;
            }
            assert!(entry.file_type().unwrap().is_file());
            let path = entry.path();
            let tag: u8 = path
                .parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .parse()
                .unwrap();
            let kind = *kinds.iter().find(|kind| **kind as u8 == tag).unwrap();
            let key = path
                .strip_prefix(directory)
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
            objects
                .push(V3ObjectRef::from_bytes(key, kind, &std::fs::read(path).unwrap()).unwrap());
            assert!(
                objects.len() <= 64,
                "tiny source unexpectedly emitted an unbounded fixture"
            );
        }
    }
    objects.sort_by(|left, right| left.key.cmp(&right.key));
    objects
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn real_imported_graph_receipt_binds_typed_members_revision_and_recovery() {
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::workspace_overlay::packed_v3::wire005::{
        AuthenticatedV3Snapshot, V3IndexReader, V3ProducerOptions, V3SourceConsistency,
        V3SourceFileLimits, V3SourceHardlinkPolicy, V3SourceNamespaceInventory,
        V3SourceNamespaceOptions,
    };
    use crate::workspace_overlay::packed_v3::{AccessProfile, PackedCodec, SizeClassTable};
    use crate::workspace_overlay::publish::binding::VerifiedPackedLower;
    use std::io::{Seek, SeekFrom, Write};
    use std::os::fd::AsRawFd;
    for mode in 0..4 {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let objects = tempfile::tempdir().unwrap();
        std::fs::create_dir(source.path().join("child")).unwrap();
        let payload = source.path().join("data");
        let mut data = std::fs::File::create(&payload).unwrap();
        data.write_all(&[17; 8192]).unwrap();
        // SAFETY: the descriptor is open, the name is terminated and the
        // value points to a live three-byte slice.
        assert_eq!(
            unsafe {
                libc::fsetxattr(
                    data.as_raw_fd(),
                    c"user.receipt".as_ptr(),
                    b"cold".as_ptr().cast(),
                    4,
                    0,
                )
            },
            0
        );
        data.sync_all().unwrap();
        std::fs::hard_link(&payload, source.path().join("child/alias")).unwrap();
        std::os::unix::fs::symlink("../data", source.path().join("child/link")).unwrap();
        let mut sparse = std::fs::File::create(source.path().join("sparse")).unwrap();
        sparse.set_len(64 << 10).unwrap();
        sparse.seek(SeekFrom::Start(4096)).unwrap();
        sparse.write_all(&[23; 4096]).unwrap();
        sparse.seek(SeekFrom::Start(48 << 10)).unwrap();
        sparse.write_all(b"tail").unwrap();
        sparse.sync_all().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(objects.path()));
        let captured = V3SourceNamespaceInventory::capture(
            source.path(),
            scratch.path(),
            V3SourceNamespaceOptions {
                root_inode: 1,
                consistency: V3SourceConsistency::BestEffortDetected,
                hardlink_policy: V3SourceHardlinkPolicy::VisibleLinks,
                file_limits: V3SourceFileLimits::default(),
            },
        )
        .await
        .unwrap()
        .build_snapshot(
            client.clone(),
            "real-import".into(),
            V3ProducerOptions {
                snapshot_id: [88; 32],
                root_dir_key: [89; 32],
                root_inode: 1,
                profile: AccessProfile::RandomSmallFile,
                size_classes: SizeClassTable::default(),
                build_policy: crate::workspace_overlay::packed_v3::wire005::V3BuildPolicy {
                    inline_data: false,
                    p90: None,
                    ..Default::default()
                },
                metadata_codec: PackedCodec::Raw,
                data_codec: PackedCodec::Raw,
            },
        )
        .await
        .unwrap();
        let snapshot = AuthenticatedV3Snapshot::open(&client, &captured.reference)
            .await
            .unwrap();
        let lower = VerifiedPackedLower::from_authenticated_snapshot(
            &snapshot,
            &V3IndexReader::new(client.clone(), 0),
        )
        .await
        .unwrap();
        let source_proof = captured.into_final_source_proof().unwrap();
        let backend = Arc::new(JournalMemoryBackend::default());
        let budget = V3MountBudget::defaults();
        let store = KvWorkspaceStore::from_arc(backend.clone())
            .with_packed_reader_pin_budget(budget.clone());
        let (_old_objects, mut publish) = setup(&store).await;
        publish.lower = lower;
        let mut journal = begin(&store, &publish, &budget).await;
        let mut references = imported_objects(objects.path());
        for kind in [
            V3ObjectKind::GroupContainer,
            V3ObjectKind::FrameDirectory,
            V3ObjectKind::ColdAttributes,
            V3ObjectKind::GroupIndex,
            V3ObjectKind::FrameIndex,
            V3ObjectKind::ColdIndex,
            V3ObjectKind::SourceStatsIndex,
        ] {
            assert!(
                references.iter().any(|reference| reference.kind == kind),
                "real namespace did not exercise {kind:?}"
            );
        }
        assert!(
            references
                .iter()
                .map(|reference| reference.object_len)
                .sum::<u64>()
                < 4 << 20
        );
        match mode {
            1 => references.retain(|reference| reference.kind != V3ObjectKind::SourceStatsIndex),
            2 => references.push(V3ObjectRef {
                key: "real-import/unreachable-extra".into(),
                kind: V3ObjectKind::ColdAttributes,
                object_len: 8192,
                digest: [91; 32],
            }),
            3 => {
                references
                    .iter_mut()
                    .find(|reference| reference.kind == V3ObjectKind::SourceStatsIndex)
                    .unwrap()
                    .kind = V3ObjectKind::ColdIndex
            }
            _ => {}
        }
        for reference in references {
            journal = store
                .register_packed_object(&journal, reference, &budget)
                .await
                .unwrap();
        }
        journal = store
            .freeze_packed_candidate(&journal, publish.record().unwrap(), &budget)
            .await
            .unwrap();
        for ordinal in 0..journal.object_count {
            journal = store
                .record_packed_object_progress(&journal, ordinal, false, &budget)
                .await
                .unwrap();
        }
        journal = store
            .advance_packed_journal(&journal, &budget)
            .await
            .unwrap();
        for ordinal in 0..journal.object_count {
            journal = store
                .record_packed_object_progress(&journal, ordinal, true, &budget)
                .await
                .unwrap();
        }
        journal = store
            .advance_packed_journal(&journal, &budget)
            .await
            .unwrap();
        let before = backend.rows.lock().await.clone();
        let proof = store
            .audit_imported_packed_graph(
                &journal,
                &source_proof,
                &client,
                &budget,
                ImportedGraphAuditOptions {
                    scratch: scratch.path(),
                    limits: V3IndexAuditLimits::default(),
                    cancel: CancellationToken::new(),
                },
            )
            .await;
        assert_eq!(
            *backend.rows.lock().await,
            before,
            "audit must only observe durable rows"
        );
        if mode != 0 {
            assert!(proof.is_err(), "bad inventory mode {mode} acquired a proof");
            continue;
        }
        let proof = proof.unwrap();
        assert!(proof.receipt.highest_inode > 1);
        assert_eq!(proof.receipt.audited_revision, journal.revision);
        assert_eq!(proof.receipt.staging_incarnation, journal.source.staging_id);
        let mut stale = journal.value.clone();
        stale.revision += 1;
        assert!(
            store
                .record_imported_packed_graph(&stale, &proof, &budget)
                .await
                .is_err()
        );
        journal = store
            .record_imported_packed_graph(&journal, &proof, &budget)
            .await
            .unwrap();
        let reopened = KvWorkspaceStore::from_arc(backend.clone())
            .with_packed_reader_pin_budget(budget.clone());
        let persisted = reopened
            .reopen_packed_journal(journal.journal_id, &journal.source, &budget)
            .await
            .unwrap();
        assert_eq!(persisted.graph_receipt, Some(proof.receipt.clone()));
        assert_eq!(
            persisted.phase,
            PackedJournalPhase::AwaitingFullProof,
            "import alone is not native publication authority"
        );
        let recovered = reopened
            .recover_packed_journal(&persisted, persisted.guard.clone(), None, &budget)
            .await
            .unwrap();
        assert!(
            recovered.graph_receipt.is_none(),
            "recovery must invalidate the old proof revision"
        );
        assert_eq!(recovered.full_proof_digest, [0; 32]);
        assert!(
            reopened
                .record_imported_packed_graph(&recovered, &proof, &budget)
                .await
                .is_err()
        );
    }
}

pub(super) async fn setup<B: WorkspaceKvBackend>(
    store: &KvWorkspaceStore<B>,
) -> (tempfile::TempDir, PublishPackedLowerBinding) {
    let (_old_dir, _old_client, _old_snapshot, old_lower, _) = packed().await;
    let initial = request(store, old_lower).await;
    let binding = store
        .install_packed_lower_binding(initial.clone())
        .await
        .unwrap();
    let (new_dir, _client, _snapshot, lower, _) = packed_with_snapshot_id([77; 32]).await;
    let guard = HeadGuard {
        expected_head_epoch: binding.head_epoch,
        ..initial.guard
    };
    let layers: [LayerRecord; 2] = store
        .load_layer_chain(guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    (
        new_dir,
        PublishPackedLowerBinding {
            guard,
            expected_layers: layers,
            expected_base: binding.base_revision.clone(),
            expected_binding: binding,
            lower,
        },
    )
}

pub(super) async fn begin<B: WorkspaceKvBackend>(
    store: &KvWorkspaceStore<B>,
    publish: &PublishPackedLowerBinding,
    budget: &Arc<V3MountBudget>,
) -> OwnedPackedJournal<PackedJournalRecord> {
    store
        .begin_packed_journal(
            JournalId::new(),
            publish.guard.clone(),
            publish.expected_layers.clone(),
            publish.expected_binding.clone(),
            source_view(),
            budget,
        )
        .await
        .unwrap()
}

async fn staging_reopen_contract<B: WorkspaceKvBackend>(backend: Arc<B>) {
    let budget = V3MountBudget::defaults();
    let store =
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    let (_directory, publish) = setup(&store).await;
    let mut journal = begin(&store, &publish, &budget).await;
    let original = journal.clone();
    let manifest = publish.lower.manifest_reference().clone();
    journal = store
        .register_packed_object(&journal, manifest.clone(), &budget)
        .await
        .unwrap();
    // A stale registration cannot mint another before-PUT dispatch guard.
    let generation = backend.get(PACKED_ROOT_GENERATION_KEY).await.unwrap();
    assert!(
        store
            .register_packed_object(&original, manifest.clone(), &budget)
            .await
            .is_err()
    );
    assert_eq!(
        backend.get(PACKED_ROOT_GENERATION_KEY).await.unwrap(),
        generation
    );
    journal = store
        .freeze_packed_candidate(&journal, publish.record().unwrap(), &budget)
        .await
        .unwrap();
    let id = journal.journal_id;
    let view = journal.source.clone();
    drop(store);
    // A new store instance uses persisted backend bytes, not a local ledger.
    let reopened =
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    assert_eq!(
        reopened
            .reopen_packed_journal(id, &view, &budget)
            .await
            .unwrap(),
        journal
    );
    let metadata_before = budget.state().used[V3BudgetPool::Metadata as usize];
    let object = reopened
        .reopen_packed_object(&journal, 0, &budget)
        .await
        .unwrap();
    assert_eq!(object.reference, manifest);
    assert!(
        object.uploaded,
        "this cfg(test) fixture explicitly simulated completion"
    );
    assert_eq!(
        budget.state().used[V3BudgetPool::Metadata as usize],
        metadata_before + RECORD_LIMIT as u64
    );
    drop(object);
    assert_eq!(
        budget.state().used[V3BudgetPool::Metadata as usize],
        metadata_before
    );
    let mut wrong_view = view.clone();
    wrong_view.frozen_view_token[0] ^= 1;
    assert!(matches!(
        reopened
            .reopen_packed_journal(id, &wrong_view, &budget)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(journal.phase, PackedJournalPhase::Uploading);
    let before = journal.clone();
    journal = reopened
        .record_packed_object_progress(&journal, 0, false, &budget)
        .await
        .unwrap();
    assert!(
        reopened
            .register_packed_object(&before, manifest.clone(), &budget)
            .await
            .is_err()
    );
    journal = reopened
        .advance_packed_journal(&journal, &budget)
        .await
        .unwrap();
    assert!(
        reopened
            .advance_packed_journal(&journal, &budget)
            .await
            .is_err()
    );
    // Real object upload/readback exists in the authenticated producer fixture;
    // these journal observations still cannot supply a complete graph seal.
    let client = crate::cadapter::client::ObjectClient::new(
        crate::cadapter::localfs::LocalFsBackend::new(_directory.path().join("objects")),
    );
    let bytes = read_v3_page(&client, &manifest, 64 << 10).await.unwrap();
    assert!(!bytes.is_empty());
    journal = reopened
        .record_packed_object_progress(&journal, 0, true, &budget)
        .await
        .unwrap();
    journal = reopened
        .advance_packed_journal(&journal, &budget)
        .await
        .unwrap();
    assert_eq!(journal.phase, PackedJournalPhase::AwaitingFullProof);
    assert_eq!(journal.full_proof_digest, [0; 32]);
    assert_eq!(
        reopened
            .load_packed_binding_record(publish.guard.clone())
            .await
            .unwrap(),
        Some(publish.expected_binding.clone())
    );
    let (roots, checks, _root_owner) = reopened.scan_packed_journal_layer_roots().await.unwrap();
    assert!(roots.contains(&publish.guard.expected_head_layer_id));
    assert!(roots.contains(&publish.expected_base.layer_id));
    assert!(backend.compare_and_swap(&checks, &[]).await.unwrap());
    let aborted = reopened
        .abort_packed_journal(&journal, "contract cancellation".into(), &budget)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .abort_packed_journal(&journal, "contract cancellation".into(), &budget)
            .await
            .unwrap(),
        aborted
    );
    assert!(!backend.compare_and_swap(&checks, &[]).await.unwrap());
    assert!(
        reopened
            .scan_packed_journal_layer_roots()
            .await
            .unwrap()
            .0
            .is_empty()
    );
    assert_eq!(
        reopened
            .reopen_packed_journal(id, &view, &budget)
            .await
            .unwrap(),
        aborted
    );
    assert!(
        reopened
            .begin_packed_journal(
                id,
                publish.guard.clone(),
                publish.expected_layers.clone(),
                publish.expected_binding.clone(),
                view,
                &budget
            )
            .await
            .is_err()
    );
    let encoded = aborted.encode().unwrap();
    for length in [0, 4, 12, encoded.len() - 1] {
        assert!(PackedJournalRecord::decode(&encoded[..length]).is_err());
    }
    let mut corrupt = encoded;
    corrupt[20] ^= 1;
    assert!(PackedJournalRecord::decode(&corrupt).is_err());
}

#[tokio::test]
async fn packed_journal_backend_reopen_pins_phases_and_abort_are_exact() {
    staging_reopen_contract(Arc::new(JournalMemoryBackend::default())).await;
}

#[tokio::test]
async fn packed_journal_commit_atomicity_with_test_only_seal_and_response_loss() {
    let backend = Arc::new(JournalMemoryBackend::default());
    let budget = V3MountBudget::defaults();
    let store =
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    let (_directory, publish) = setup(&store).await;
    let mut journal = begin(&store, &publish, &budget).await;
    journal = store
        .register_packed_object(
            &journal,
            publish.lower.manifest_reference().clone(),
            &budget,
        )
        .await
        .unwrap();
    journal = store
        .freeze_packed_candidate(&journal, publish.record().unwrap(), &budget)
        .await
        .unwrap();
    journal = store
        .record_packed_object_progress(&journal, 0, false, &budget)
        .await
        .unwrap();
    journal = store
        .advance_packed_journal(&journal, &budget)
        .await
        .unwrap();
    journal = store
        .record_packed_object_progress(&journal, 0, true, &budget)
        .await
        .unwrap();
    journal = store
        .advance_packed_journal(&journal, &budget)
        .await
        .unwrap();
    // Descendant tests alone can build these tokens. This test exercises CAS
    // atomicity only; the real source/graph tests below issue production proofs.
    let imported = ImportedPackedGraphSeal {
        receipt: PackedGraphReceipt {
            audited_revision: journal.revision,
            staging_incarnation: journal.source.staging_id,
            manifest: publish.lower.manifest_reference().clone(),
            object_count: journal.object_count,
            inventory_digest: journal.inventory_digest,
            physical_graph_digest: [70; 32],
            final_source_digest: [71; 32],
            catalog_context_digest: audit_basis_digest(&journal, journal.revision).unwrap(),
            snapshot_backed: journal.source.snapshot_backed,
            highest_inode: publish.lower.highest_inode() as u64,
        },
        _permit: budget.admit(&[(V3BudgetPool::Metadata, 32 << 10)]).unwrap(),
    };
    journal = store
        .record_imported_packed_graph(&journal, &imported, &budget)
        .await
        .unwrap();
    let seal = CompletePackedGraphSeal {
        journal_id: journal.journal_id,
        source: journal.source.clone(),
        manifest: publish.lower.manifest_reference().clone(),
        object_count: journal.object_count,
        inventory_digest: journal.inventory_digest,
        proof_digest: [99; 32],
        guard: journal.guard.clone(),
        audited_revision: imported.receipt.audited_revision,
        staging_incarnation: journal.source.staging_id,
        graph_receipt_digest: imported.receipt.digest().unwrap(),
    };
    journal = store
        .record_packed_full_proof(&journal, &seal, &budget)
        .await
        .unwrap();
    let before = backend.rows.lock().await.clone();
    backend.expire_at_cas.store(true, Ordering::SeqCst);
    assert!(matches!(
        store
            .commit_packed_journal(&journal, &publish, &seal, &budget)
            .await,
        Err(WorkspaceError::Fenced)
    ));
    assert_eq!(*backend.rows.lock().await, before);
    backend.now.store(1, Ordering::SeqCst);
    backend.lose_reply.store(true, Ordering::SeqCst);
    assert!(
        store
            .commit_packed_journal(&journal, &publish, &seal, &budget)
            .await
            .is_err()
    );
    let committed_bytes = backend.rows.lock().await.clone();
    let other =
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    let committed = other
        .commit_packed_journal(&journal, &publish, &seal, &budget)
        .await
        .unwrap();
    assert_eq!(*backend.rows.lock().await, committed_bytes);
    assert_eq!(committed.phase, PackedJournalPhase::Committed);
    let target = publish.record().unwrap();
    let guard = HeadGuard {
        expected_head_epoch: target.head_epoch,
        ..publish.guard.clone()
    };
    assert_eq!(
        other.load_packed_binding_record(guard).await.unwrap(),
        Some(target.clone())
    );
    assert_eq!(
        other
            .load_packed_binding_version(target.workspace_id, target.binding.binding_version)
            .await
            .unwrap(),
        Some(target)
    );
    assert!(
        other
            .scan_packed_journal_layer_roots()
            .await
            .unwrap()
            .0
            .is_empty()
    );
    assert!(
        other
            .abort_packed_journal(
                &committed,
                "cannot undo committed publication".into(),
                &budget
            )
            .await
            .is_err()
    );

    // A terminal journal retry must reject a retained root that still carries
    // an outstanding PUT hold. This simulates a crash-visible partial root
    // and prevents recovery from acknowledging incomplete publication.
    other
        .test_set_registry_root_pending_puts(journal.source.staging_id, 1)
        .await
        .unwrap();
    assert!(matches!(
        other
            .commit_packed_journal(&journal, &publish, &seal, &budget)
            .await,
        Err(WorkspaceError::Busy)
    ));
}

#[tokio::test]
async fn packed_journal_abort_retry_allows_dispatched_pending_put() {
    let backend = Arc::new(JournalMemoryBackend::default());
    let budget = V3MountBudget::defaults();
    let store =
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    let (_directory, publish) = setup(&store).await;
    let journal = begin(&store, &publish, &budget).await;
    let (reserved, guard) = store
        .reserve_packed_upload(
            &journal,
            publish.lower.manifest_reference().clone(),
            &budget,
        )
        .await
        .unwrap();

    // Dispatch is a durable single-use transition. A lost reply leaves the
    // PUT marked dispatched while retaining its pending hold.
    backend.lose_reply.store(true, Ordering::SeqCst);
    assert!(
        store
            .dispatch_packed_upload(&reserved, &guard, &budget)
            .await
            .is_err()
    );
    let dispatched = store
        .reopen_packed_journal(reserved.journal_id, &reserved.source, &budget)
        .await
        .unwrap();

    // Aborting after that crash retains the pending hold. The abort CAS may
    // also lose its reply; retry must acknowledge the exact AbortedRetained
    // root without requiring pending_puts to reach zero.
    backend.lose_reply.store(true, Ordering::SeqCst);
    assert!(
        store
            .abort_packed_journal(&dispatched, "dispatch reply lost".into(), &budget)
            .await
            .is_err()
    );
    let aborted = store
        .abort_packed_journal(&dispatched, "dispatch reply lost".into(), &budget)
        .await
        .unwrap();
    assert_eq!(aborted.phase, PackedJournalPhase::Aborted);
}

#[tokio::test]
async fn packed_journal_same_holder_reattach_fences_previous_revision_and_old_gc_snapshot() {
    let backend = Arc::new(JournalMemoryBackend::default());
    let budget = V3MountBudget::defaults();
    let store =
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone());
    let (_directory, publish) = setup(&store).await;
    let journal = begin(&store, &publish, &budget).await;
    let (_, checks, _root_owner) = store.scan_packed_journal_layer_roots().await.unwrap();
    // Reattach this ordinary journal under its actual still-live holder.
    // Packed owner takeover uses the scoped Prepare/Q recovery contracts;
    // generic release/acquire cannot issue a packed administrative owner.
    let guard = publish.guard.clone();
    let recovered = store
        .recover_packed_journal(&journal, guard.clone(), None, &budget)
        .await
        .unwrap();
    assert_eq!(recovered.guard, journal.guard);
    assert_eq!(recovered.revision, journal.revision + 1);
    assert_eq!(
        store
            .recover_packed_journal(&journal, guard, None, &budget)
            .await
            .unwrap(),
        recovered
    );
    assert!(
        store
            .register_packed_object(
                &journal,
                publish.lower.manifest_reference().clone(),
                &budget
            )
            .await
            .is_err()
    );
    assert!(!backend.compare_and_swap(&checks, &[]).await.unwrap());
    assert!(
        store
            .register_packed_object(
                &recovered,
                publish.lower.manifest_reference().clone(),
                &budget
            )
            .await
            .is_ok()
    );
}

// Remote tests own a fresh UUID namespace. Capture panics, remove only that
#[tokio::test]
async fn packed_journal_changed_head_can_only_abort_the_old_candidate() {
    let backend = Arc::new(JournalMemoryBackend::default());
    let budget = V3MountBudget::defaults();
    let store = KvWorkspaceStore::from_arc(backend).with_packed_reader_pin_budget(budget.clone());
    let (_directory, publish) = setup(&store).await;
    let journal = begin(&store, &publish, &budget).await;
    let winner = store
        .publish_packed_lower_binding(publish.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: winner.head_epoch,
        ..publish.guard.clone()
    };
    assert!(
        store
            .recover_packed_journal(&journal, guard.clone(), None, &budget)
            .await
            .is_err()
    );
    let aborted = store
        .recover_packed_journal(
            &journal,
            guard.clone(),
            Some("head advanced; abort only".into()),
            &budget,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .recover_packed_journal(
                &journal,
                guard.clone(),
                Some("head advanced; abort only".into()),
                &budget
            )
            .await
            .unwrap(),
        aborted
    );
    assert_eq!(aborted.phase, PackedJournalPhase::Aborted);
    assert_eq!(
        store.load_packed_binding_record(guard).await.unwrap(),
        Some(winner)
    );
}

// Remote tests own a fresh UUID namespace. Capture panics, remove only that
// backend namespace's keys, then propagate the failure. No shared services stop.
async fn remote_contract<B: WorkspaceKvBackend>(backend: B) {
    let backend = Arc::new(backend);
    let run_backend = backend.clone();
    let outcome = tokio::spawn(async move { staging_reopen_contract(run_backend).await }).await;
    let entries = backend.scan_prefix(b"").await.unwrap();
    let checks = entries
        .iter()
        .map(|entry| KvCheck {
            key: entry.key.clone(),
            expected: Some(entry.value.clone()),
        })
        .collect::<Vec<_>>();
    let writes = entries
        .iter()
        .map(|entry| KvWrite::Delete {
            key: entry.key.clone(),
        })
        .collect::<Vec<_>>();
    assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
    assert!(backend.scan_prefix(b"").await.unwrap().is_empty());
    outcome.expect("remote journal contract failed after namespace cleanup");
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_REDIS_URL"]
async fn real_redis_packed_journal_reopen_staging_and_abort() {
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL missing");
    let namespace = format!("g12-journal-{}", Uuid::new_v4().simple());
    remote_contract(
        RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap(),
    )
    .await;
}
#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_packed_journal_reopen_staging_and_abort() {
    let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
        .expect("BREWFS_TEST_TIKV_PD_ENDPOINTS missing")
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect();
    let namespace = format!("g12-journal-{}", Uuid::new_v4().simple());
    remote_contract(
        TiKvWorkspaceBackend::connect(endpoints, &namespace)
            .await
            .unwrap(),
    )
    .await;
}
