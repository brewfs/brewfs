//! Real VFS -> recovered native authority -> producer -> publication contracts.
//! Only backend scheduling changes; no source, phase proof or graph is invented.

use super::*;
use crate::workspace::packed_reader_lifecycle::{KvPackedReaderSession, PackedReaderSession};
use crate::workspace::stores::kv_store::packed_journal::tests::JournalMemoryBackend;
use crate::workspace::stores::kv_store::packed_native_freeze::NativePrepareRecoveryRequest;
use std::collections::BTreeMap;
use std::time::Duration;

const MEMBER_PREFIX: &[u8] = b"packed/registry/member/";
const PAYLOAD: &[u8] = b"packed actual root-conflict native bytes";
const NOW: i64 = 1_000_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stage {
    Reserve,
    Dispatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    RootOnly,
    RootChurn,
    ChangedPpj,
    ChangedLease,
    ChangedOpen,
    CommittedReplyLost,
}

#[derive(Clone)]
struct Packet {
    checks: Vec<KvCheck>,
    writes: Vec<KvWrite>,
    deadline: i64,
    previous: PackedJournalRecord,
    next: PackedJournalRecord,
}

struct Schedule {
    stage: Stage,
    fault: Fault,
    packets: Vec<Packet>,
    commits: usize,
    false_results: usize,
    after_fault: Option<BTreeMap<Vec<u8>, Vec<u8>>>,
}

#[derive(Default)]
struct RootConflictBackend {
    memory: JournalMemoryBackend,
    schedule: Mutex<Option<Schedule>>,
}

impl RootConflictBackend {
    fn packet(checks: &[KvCheck], writes: &[KvWrite], deadline: i64) -> Option<(Stage, Packet)> {
        let next = writes.iter().find_map(|write| match write {
            KvWrite::Put { key, value } if key.starts_with(JOURNAL_PREFIX) => {
                let record = PackedJournalRecord::decode(value).ok()?;
                (record.phase == PackedJournalPhase::Building
                    && record.native_rebind.is_some()
                    && *key == journal_key(record.journal_id))
                .then_some(record)
            }
            _ => None,
        })?;
        let previous = checks.iter().find_map(|check| {
            (check.key == journal_key(next.journal_id))
                .then(|| {
                    check
                        .expected
                        .as_deref()
                        .and_then(|raw| PackedJournalRecord::decode(raw).ok())
                })
                .flatten()
        })?;
        if previous.phase != PackedJournalPhase::Building
            || previous.native_rebind.is_none()
            || !writes.iter().any(
                |write| matches!(write, KvWrite::Put { key, .. } if key.starts_with(MEMBER_PREFIX)),
            )
        {
            return None;
        }
        let stage = if next.object_count == previous.object_count + 1 {
            Stage::Reserve
        } else if next.object_count == previous.object_count
            && !writes.iter().any(|write| {
                matches!(write, KvWrite::Put { key, .. }
                    if key.starts_with(b"packed/staging/")
                        && key.windows(b"/objects/".len()).any(|window| window == b"/objects/"))
            })
        {
            Stage::Dispatch
        } else {
            return None;
        };
        Some((
            stage,
            Packet {
                checks: checks.to_vec(),
                writes: writes.to_vec(),
                deadline,
                previous,
                next,
            },
        ))
    }

    async fn arm(&self, stage: Stage, fault: Fault) {
        *self.schedule.lock().await = Some(Schedule {
            stage,
            fault,
            packets: Vec::new(),
            commits: 0,
            false_results: 0,
            after_fault: None,
        });
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
        // This lock gives the same single commit version for check/time/write.
        let mut rows = self.memory.rows.lock().await;
        let mut schedule = self.schedule.lock().await;
        let mut target = false;
        let mut first = false;
        if let (Some(schedule), Some(deadline)) = (schedule.as_mut(), upper)
            && let Some((stage, packet)) = Self::packet(checks, writes, deadline)
            && stage == schedule.stage
            && schedule.packets.first().is_none_or(|first| {
                first.next.journal_id == packet.next.journal_id
                    && first.next.revision == packet.next.revision
            })
        {
            target = true;
            first = schedule.packets.is_empty();
            assert!(
                checks
                    .iter()
                    .all(|check| { rows.get(&check.key) == check.expected.as_ref() }),
                "producer packet already mismatched before the scheduled event"
            );
            assert!(lower.is_none_or(|bound| NOW >= bound));
            assert!(NOW < deadline);
            assert!(
                checks
                    .iter()
                    .any(|check| check.key == PACKED_ROOT_GENERATION_KEY)
            );
            schedule.packets.push(packet);
            if (first && schedule.fault != Fault::CommittedReplyLost)
                || schedule.fault == Fault::RootChurn
            {
                let actual = rows.get(PACKED_ROOT_GENERATION_KEY).cloned();
                rows.insert(
                    PACKED_ROOT_GENERATION_KEY.to_vec(),
                    encode(&next_packed_root_generation(&actual)?)?,
                );
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
        let matched = lower.is_none_or(|bound| NOW >= bound)
            && upper.is_none_or(|bound| NOW < bound)
            && checks
                .iter()
                .all(|check| rows.get(&check.key) == check.expected.as_ref());
        if !matched {
            if target {
                let schedule = schedule.as_mut().unwrap();
                schedule.false_results += 1;
                if first {
                    let packet = schedule.packets.last().unwrap();
                    // The real CAS has now failed on the bumped epoch. These
                    // changes happen before its false result reaches rebuild.
                    match schedule.fault {
                        Fault::ChangedPpj => {
                            let concurrent = packet.previous.next()?.encode()?;
                            rows.insert(
                                journal_key(packet.previous.journal_id),
                                concurrent.clone(),
                            );
                            rows.insert(active_key(packet.previous.journal_id), concurrent);
                        }
                        Fault::ChangedLease => {
                            let key = packet
                                .checks
                                .iter()
                                .find(|check| {
                                    check.key.starts_with(HOT_LEASE_PREFIX)
                                        && check.expected.is_some()
                                })
                                .expect("actual native packet checks the current lease")
                                .key
                                .clone();
                            let mut lease: SnapshotLease =
                                decode_open_value(rows.get(&key).unwrap(), 48 << 10)?;
                            lease.expires_at_ns += 1;
                            rows.insert(key, encode(&lease)?);
                        }
                        Fault::ChangedOpen => {
                            let key = open_snapshot_key(packet.previous.guard.workspace_id);
                            assert!(packet.checks.iter().any(|check| check.key == key));
                            let mut open: SnapshotOpenRecord =
                                decode_open_value(rows.get(&key).unwrap(), OPEN_RECORD_MAX_BYTES)?;
                            open.expires_at_ns += 1;
                            rows.insert(key, encode(&open)?);
                        }
                        Fault::RootOnly | Fault::RootChurn => {}
                        Fault::CommittedReplyLost => unreachable!(),
                    }
                    schedule.after_fault = Some(rows.clone());
                }
                if schedule.fault == Fault::RootChurn {
                    schedule.after_fault = Some(rows.clone());
                }
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
        if target {
            let schedule = schedule.as_mut().unwrap();
            schedule.commits += 1;
            if first && schedule.fault == Fault::CommittedReplyLost {
                schedule.after_fault = Some(rows.clone());
                return Err(WorkspaceError::Backend(
                    "actual committed producer CAS reply lost".into(),
                ));
            }
        }
        Ok(true)
    }
}

#[async_trait]
impl WorkspaceKvBackend for RootConflictBackend {
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    fn name(&self) -> &'static str {
        "packed-root-conflict-memory-test"
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
        Ok((self.memory.get_many_consistent(keys).await?, NOW))
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let (values, _) = self
            .memory
            .get_many_consistent_with_time_bounded(keys, limits)
            .await?;
        Ok((values, NOW))
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.memory.scan_prefix(prefix).await
    }
    async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        max: usize,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.memory.scan_prefix_bounded(prefix, max).await
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
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
        Ok(NOW)
    }
}

fn reader_options() -> PackedReaderLeaseOptions {
    PackedReaderLeaseOptions {
        ttl_ns: 900_000_000_000,
        ..Default::default()
    }
}

// Each real mount/recovery scope explicitly owns one finite canonical ledger.
// Store writer reads, pin session, lower metadata and native recovery use this Arc.
fn conflict_mount_budget() -> Arc<SnapshotMountBudget> {
    SnapshotMountBudget::new(crate::workspace::packed::snapshot::SnapshotBudgetLimits {
        bytes: [
            16 << 20,
            256 << 20,
            32 << 20,
            1 << 20,
            32 << 20,
            32 << 20,
            8 << 20,
            32 << 20,
        ],
        max_read_bytes: 4 << 20,
    })
    .unwrap()
}

async fn pipeline(stage: Stage, fault: Fault) {
    let (_objects, client, snapshot, lower_proof, _) = packed().await;
    let scratch = tempfile::tempdir().unwrap();
    let scheduled = Arc::new(RootConflictBackend::default());
    let backend = Arc::new(FinalDelivery::new(scheduled.clone()));
    let old_budget = conflict_mount_budget();
    let original = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone())
            .with_packed_reader_pin_budget(old_budget.clone()),
    );
    let install = request(original.as_ref(), lower_proof).await;
    let binding = original
        .install_packed_lower_binding(install.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: binding.head_epoch,
        ..install.guard
    };
    original
        .renew_lease(RenewLease {
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
            ttl_ns: 300_000_000_000,
        })
        .await
        .unwrap();
    let old_reader = original
        .clone()
        .open_packed_reader_session(guard.clone(), old_budget.clone(), reader_options())
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&old_budget, &old_reader.mount_budget()));
    let layout = ChunkLayout {
        chunk_size: 4096,
        block_size: 4096,
    };
    let upper = Arc::new(InMemoryBlockStore::new());
    let lower = Arc::new(
        PackedReadonlyMeta::from_snapshot_budget(
            client.clone(),
            snapshot,
            4096,
            0,
            old_budget.clone(),
        )
        .unwrap(),
    );
    let old_meta = Arc::new(
        WorkspaceMetaLayer::with_chunk_size(
            original.clone(),
            ViewContext {
                workspace_id: guard.workspace_id,
                head_layer_id: guard.expected_head_layer_id,
                head_epoch: guard.expected_head_epoch,
                lease_id: guard.lease_id,
                holder_generation: guard.holder_generation,
            },
            4096,
        )
        .with_packed_lower(
            binding.binding.clone(),
            lower,
            Arc::new(PinnedCatalogPackedBindingAuthority {
                store: original.clone(),
                reader: old_reader.clone(),
            }),
            upper.clone(),
            layout,
        )
        .unwrap(),
    );
    old_meta.initialize().await.unwrap();
    let provider: Arc<dyn WorkspaceReadPlanProvider> = old_meta.clone();
    let old_vfs = VFS::from_readonly_components_with_provider(
        VFSConfig::new(layout),
        upper.clone(),
        old_meta.clone(),
        provider,
    )
    .unwrap();
    let inode = old_vfs.create_file("/root-conflict-native").await.unwrap();
    let handle = old_vfs
        .open(
            inode,
            old_vfs.stat("/root-conflict-native").await.unwrap(),
            true,
            true,
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        old_vfs.write(handle, 0, PAYLOAD).await.unwrap(),
        PAYLOAD.len()
    );
    old_vfs.flush(handle).await.unwrap();
    old_vfs.close(handle).await.unwrap();
    let local = old_vfs.quiesce_packed_vfs().await.unwrap();
    let layers: [LayerRecord; 2] = original
        .load_layer_chain(guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let native_journal = JournalId::new();
    let planned_head = LayerId::new();
    backend.stop_original_seed_q.store(true, Ordering::SeqCst);
    assert!(
        original
            .clone()
            .begin_packed_native_quiesce(
                guard.clone(),
                layers.clone(),
                native_journal,
                planned_head,
                old_budget.clone(),
            )
            .await
            .is_err()
    );
    let control = test_entity_state(backend.as_ref()).await;
    assert_eq!(control.journals[&native_journal].phase, SealPhase::Prepare);
    drop(local);
    drop(old_vfs);
    drop(old_meta);
    old_reader.shutdown().await.unwrap();
    drop(old_reader);
    drop(old_budget);
    drop(original);

    // Consume a real recovered Prepare owner so lease/open/claim checks are
    // all part of the producer packet, without requiring Q-only takeover.
    let budget = conflict_mount_budget();
    let store = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    let original_open: SnapshotOpenRecord = decode_open_value(
        &backend
            .get(&open_snapshot_key(guard.workspace_id))
            .await
            .unwrap()
            .unwrap(),
        OPEN_RECORD_MAX_BYTES,
    )
    .unwrap();
    assert_eq!(original_open.state, SnapshotOpenState::Ready);
    assert!(!original_open.recovery_required);
    // Reattach the actual Prepare owner while its original lease is live.
    // This preserves the original guard; no expired-owner takeover is claimed.
    let owner = original_open.owner_id.clone();
    let open = store
        .open_workspace_snapshot(guard.workspace_id, owner.clone(), Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(open.state, SnapshotOpenState::Recovering);
    assert!(open.recovery_required);
    assert_eq!(open.generation, original_open.generation);
    let native = store
        .recover_packed_native_prepare(
            NativePrepareRecoveryRequest {
                journal_id: native_journal,
                owner_id: owner,
                new_lease_id: None,
                ttl_ns: 300_000_000_000,
            },
            budget.clone(),
        )
        .await
        .unwrap();
    assert_eq!(native.mapping().old_guard(), &guard);
    assert_eq!(native.mapping().old_layers(), &layers);
    assert_eq!(native.mapping().planned_head_layer_id(), planned_head);
    assert_eq!(native.source_guard(), &guard);
    let original_q = native.canonical_receipt_bytes().to_vec();
    let reader: Arc<dyn PackedReaderSession> = KvPackedReaderSession::open_native_prepare(
        &store,
        native.clone(),
        budget.clone(),
        reader_options(),
    )
    .await
    .unwrap();
    reader.validate().await.unwrap();
    let snapshot = AuthenticatedSnapshot::open(&client, &binding.binding.manifest)
        .await
        .unwrap();
    let lower = Arc::new(
        PackedReadonlyMeta::from_snapshot_budget(client.clone(), snapshot, 4096, 0, budget.clone())
            .unwrap(),
    );
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
    let provider: Arc<dyn WorkspaceReadPlanProvider> = meta.clone();
    let vfs = VFS::from_readonly_components_with_provider(
        VFSConfig::new(layout),
        upper.clone(),
        meta.clone(),
        provider,
    )
    .unwrap();
    let local = vfs.quiesce_packed_vfs().await.unwrap();
    let artifact = FrozenNativeArtifact::capture(
        native.clone(),
        local,
        scratch.path().to_path_buf(),
        capture_limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    scheduled.arm(stage, fault).await;
    let prepared = store
        .prepare_frozen_native_publication(
            artifact,
            client.clone(),
            NativePublicationBuildOptions {
                producer: producer_options(),
                temporary: scratch.path().to_path_buf(),
                graph_scratch: scratch.path().to_path_buf(),
                graph_limits: SnapshotIndexAuditLimits::default(),
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
        .await;

    if fault == Fault::RootOnly {
        let ready = match prepared {
            Ok(ready) => ready,
            Err(error) => panic!(
                "actual root-only native factory failed: {}",
                preparation_error(&error)
            ),
        };
        assert_eq!(ready.record.phase, PackedJournalPhase::Verified);
        assert_eq!(
            ready.source.phase_authority().canonical_receipt_bytes(),
            original_q
        );
        let staged = ready.record.value.clone();
        let plan = staged
            .native_rebind
            .as_ref()
            .unwrap()
            .publication
            .as_ref()
            .unwrap()
            .clone();
        let target = staged.commit_target.clone().unwrap();
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
        let outcome = match ready.commit().await {
            Ok(outcome) => outcome,
            Err(failure) => panic!("actual root-only publication failed: {}", failure.error),
        };
        assert_eq!(outcome.binding, target);
        assert_eq!(outcome.guard.expected_head_layer_id, planned_head);
        assert_eq!(backend.final_calls.load(Ordering::SeqCst), 1);
        let committed = PackedJournalRecord::decode(
            &backend
                .get(&journal_key(staged.journal_id))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(committed.phase, PackedJournalPhase::Committed);
        assert!(
            backend
                .get(&active_key(staged.journal_id))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            active_count(&backend.get(ACTIVE_COUNT_KEY).await.unwrap()).unwrap(),
            0
        );
        let pair: [LayerRecord; 2] = store
            .load_layer_chain(planned_head)
            .await
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(pair[0].state, LayerState::Writable);
        assert_eq!(pair[0].depth, 2);
        assert_eq!(pair[1].layer_id, plan.carrier_layer_id);
        assert_eq!(pair[1].state, LayerState::Sealed);
        assert_eq!(pair[1].depth, 1);
        assert_eq!(pair[1].parent_layer_id, None);
        let sealed = store
            .load_layer(guard.expected_head_layer_id)
            .await
            .unwrap();
        assert_eq!(sealed.state, LayerState::Sealed);
        assert_eq!(sealed.delta_digest, Some(source_digest));
        assert_eq!(sealed.root_hash, Some(source_root));
        assert_eq!(
            store.load_layer(layers[1].layer_id).await.unwrap(),
            layers[1]
        );
        let published_open: SnapshotOpenRecord = decode_open_value(
            &backend
                .get(&open_snapshot_key(guard.workspace_id))
                .await
                .unwrap()
                .unwrap(),
            OPEN_RECORD_MAX_BYTES,
        )
        .unwrap();
        assert_eq!(published_open.generation, open.generation);
        assert_eq!(published_open.state, SnapshotOpenState::Ready);
        assert!(!published_open.recovery_required);
        assert!(
            backend
                .get(&open_snapshot_recovery_key(guard.workspace_id))
                .await
                .unwrap()
                .is_none()
        );
        let authenticated = AuthenticatedSnapshot::open(&client, &target.binding.manifest)
            .await
            .unwrap();
        let candidate = PackedReadonlyMeta::from_snapshot_budget(
            client.clone(),
            authenticated,
            4096,
            0,
            budget.clone(),
        )
        .unwrap();
        assert_eq!(
            candidate.stat(inode).await.unwrap().unwrap().size as usize,
            PAYLOAD.len()
        );
        let read = candidate
            .prepare_unified_read(inode, 0, 0, PAYLOAD.len() as u64)
            .await
            .unwrap()
            .unwrap();
        let mut bytes = vec![0; PAYLOAD.len()];
        execute_unified_into(read.fetcher.as_ref(), 0, &read.plan, &mut bytes)
            .await
            .unwrap();
        assert_eq!(bytes, PAYLOAD);
        drop((read, candidate, outcome));
        let schedule_guard = scheduled.schedule.lock().await;
        let schedule = schedule_guard.as_ref().unwrap();
        assert_eq!(schedule.false_results, 1);
        assert_eq!(schedule.commits, 1);
        assert_eq!(schedule.packets.len(), 2);
        let first = &schedule.packets[0];
        let second = &schedule.packets[1];
        assert!(second.deadline <= first.deadline);
        assert!(second.deadline <= open.expires_at_ns);
        assert_eq!(first.next, second.next);
        for check in &first.checks {
            let fresh = second
                .checks
                .iter()
                .find(|fresh| fresh.key == check.key)
                .expect("fresh packet omitted a condition");
            if check.key == PACKED_ROOT_GENERATION_KEY {
                assert_ne!(fresh.expected, check.expected);
            } else {
                assert_eq!(
                    fresh.expected, check.expected,
                    "fresh native mutation broadened a stable condition"
                );
            }
        }
        assert_eq!(first.checks.len(), second.checks.len());
        let stable_writes = |writes: &[KvWrite]| {
            writes.iter().filter(|write| {
            !matches!(write, KvWrite::Put { key, .. } if key.as_slice() == PACKED_ROOT_GENERATION_KEY)
        }).cloned().collect::<Vec<_>>()
        };
        assert_eq!(stable_writes(&first.writes), stable_writes(&second.writes));
    } else {
        let error = match prepared {
            Ok(_) => panic!("changed/unknown producer packet unexpectedly completed"),
            Err(error) => error,
        };
        assert!(matches!(
            preparation_error(&error),
            WorkspaceError::Busy | WorkspaceError::Fenced | WorkspaceError::Backend(_)
        ));
        if fault == Fault::CommittedReplyLost {
            assert!(
                preparation_error(&error)
                    .to_string()
                    .contains("actual committed producer CAS reply lost")
            );
        }
        drop(error);
        assert_eq!(backend.final_calls.load(Ordering::SeqCst), 0);
        let rows = scheduled.memory.rows.lock().await;
        let schedule_guard = scheduled.schedule.lock().await;
        let schedule = schedule_guard.as_ref().unwrap();
        let attempts = if fault == Fault::RootChurn { 3 } else { 1 };
        assert_eq!(
            schedule.packets.len(),
            attempts,
            "failed/unknown mutation exceeded the exact attempt bound"
        );
        assert_eq!(
            schedule.commits,
            usize::from(fault == Fault::CommittedReplyLost)
        );
        assert_eq!(
            schedule.false_results,
            if fault == Fault::CommittedReplyLost {
                0
            } else {
                attempts
            }
        );
        assert_eq!(
            &*rows,
            schedule.after_fault.as_ref().unwrap(),
            "factory changed durable rows after failed/unknown mutation"
        );
        if fault == Fault::RootChurn {
            for pair in schedule.packets.windows(2) {
                assert_eq!(pair[0].next, pair[1].next);
                assert!(pair[1].deadline <= pair[0].deadline);
                assert_eq!(pair[0].checks.len(), pair[1].checks.len());
                for check in &pair[0].checks {
                    let fresh = pair[1]
                        .checks
                        .iter()
                        .find(|fresh| fresh.key == check.key)
                        .unwrap();
                    if check.key == PACKED_ROOT_GENERATION_KEY {
                        assert_ne!(fresh.expected, check.expected);
                    } else {
                        assert_eq!(fresh.expected, check.expected);
                    }
                }
            }
        }
        let packet = &schedule.packets[0];
        let durable = PackedJournalRecord::decode(
            rows.get(&journal_key(packet.previous.journal_id)).unwrap(),
        )
        .unwrap();
        assert_eq!(durable.phase, PackedJournalPhase::Building);
        assert_eq!(
            rows.get(&active_key(durable.journal_id)),
            rows.get(&journal_key(durable.journal_id))
        );
        assert_eq!(
            rows.get(&packed_current_key(guard.workspace_id)),
            Some(&binding.encode().unwrap())
        );
        assert!(!rows.contains_key(&hot_layer_key(planned_head)));
        assert!(
            !rows.contains_key(&hot_layer_key(
                durable
                    .native_rebind
                    .as_ref()
                    .unwrap()
                    .publication
                    .as_ref()
                    .unwrap()
                    .carrier_layer_id
            ))
        );
        assert_eq!(
            active_count(&rows.get(ACTIVE_COUNT_KEY).cloned()).unwrap(),
            1
        );
        if fault == Fault::CommittedReplyLost || stage == Stage::Dispatch {
            let occurrence =
                PackedJournalObject::decode(rows.get(&object_key(durable.journal_id, 0)).unwrap())
                    .unwrap();
            assert!(!occurrence.uploaded);
            assert!(!occurrence.readback_recorded);
            let reference = occurrence.reference.clone();
            let member_write = packet
                .writes
                .iter()
                .find_map(|write| match write {
                    KvWrite::Put { key, .. } if key.starts_with(MEMBER_PREFIX) => Some(key),
                    _ => None,
                })
                .unwrap();
            let mut cursor = JournalCursor::checked(
                rows.get(member_write).unwrap(),
                b"PRM3",
                REFERENCE_LIMIT + 512,
            )
            .unwrap();
            assert_eq!(
                SnapshotObjectRef::decode_value(&cursor.bytes(REFERENCE_LIMIT).unwrap()).unwrap(),
                reference
            );
            let _: [u8; 16] = cursor.take().unwrap();
            let _: [u8; 16] = cursor.take().unwrap();
            assert_eq!(cursor.u64().unwrap(), 0);
            let put_id: [u8; 16] = cursor.take().unwrap();
            assert_ne!(put_id, [0; 16]);
            assert_eq!(cursor.take::<1>().unwrap(), [0]);
            assert_eq!(cursor.take::<1>().unwrap(), [1]);
            assert_eq!(
                cursor.take::<1>().unwrap(),
                [u8::from(
                    stage == Stage::Dispatch && fault == Fault::CommittedReplyLost
                )]
            );
            assert_eq!(cursor.take::<1>().unwrap(), [1]);
            cursor.end().unwrap();
            drop(schedule_guard);
            drop(rows);
            assert!(
                client.get_object(&reference.key).await.unwrap().is_none(),
                "unknown reserve/dispatch response authorized a physical PUT"
            );
        } else {
            assert_eq!(durable.object_count, 0);
        }
    }
    drop(native);
    drop(vfs);
    drop(meta);
    // The reader owns only actual pin rows. A concurrently fenced source may
    // reject its release; the Memory namespace is local to this test either way.
    let _ = reader.shutdown().await;
}

#[tokio::test]
async fn actual_memory_native_producer_reserve_root_conflict_rebuilds_and_publishes() {
    pipeline(Stage::Reserve, Fault::RootOnly).await;
}
#[tokio::test]
async fn actual_memory_native_producer_dispatch_root_conflict_rebuilds_and_publishes() {
    pipeline(Stage::Dispatch, Fault::RootOnly).await;
}
#[tokio::test]
async fn actual_memory_native_producer_reserve_root_churn_stops_after_three_mutation_attempts() {
    pipeline(Stage::Reserve, Fault::RootChurn).await;
}
#[tokio::test]
async fn actual_memory_native_producer_dispatch_root_churn_stops_after_three_mutation_attempts() {
    pipeline(Stage::Dispatch, Fault::RootChurn).await;
}
#[tokio::test]
async fn actual_memory_native_producer_reserve_changed_ppj_rejects_before_second_mutation() {
    pipeline(Stage::Reserve, Fault::ChangedPpj).await;
}
#[tokio::test]
async fn actual_memory_native_producer_dispatch_changed_ppj_rejects_before_second_mutation() {
    pipeline(Stage::Dispatch, Fault::ChangedPpj).await;
}
#[tokio::test]
async fn actual_memory_native_producer_reserve_changed_lease_rejects_before_second_mutation() {
    pipeline(Stage::Reserve, Fault::ChangedLease).await;
}
#[tokio::test]
async fn actual_memory_native_producer_dispatch_changed_lease_rejects_before_second_mutation() {
    pipeline(Stage::Dispatch, Fault::ChangedLease).await;
}
#[tokio::test]
async fn actual_memory_native_producer_reserve_changed_open_rejects_before_second_mutation() {
    pipeline(Stage::Reserve, Fault::ChangedOpen).await;
}
#[tokio::test]
async fn actual_memory_native_producer_dispatch_changed_open_rejects_before_second_mutation() {
    pipeline(Stage::Dispatch, Fault::ChangedOpen).await;
}
#[tokio::test]
async fn actual_memory_native_producer_reserve_unknown_commit_never_reissues_mutation_or_put() {
    pipeline(Stage::Reserve, Fault::CommittedReplyLost).await;
}
#[tokio::test]
async fn actual_memory_native_producer_dispatch_unknown_commit_never_reissues_mutation_or_put() {
    pipeline(Stage::Dispatch, Fault::CommittedReplyLost).await;
}
