//! Actual native publication -> public forks -> pinned readers -> child-only
//! retirement, including a real child repack and real nonterminal journal.

use super::*;
use crate::cadapter::localfs::LocalFsBackend;
use crate::chunk::cache::ChunksCacheConfig;
use crate::chunk::compress::Compression;
use crate::chunk::store::{BlockStoreConfig, ObjectBlockStore};
use crate::workspace::catalog::{DeleteLayerMetadata, MarkDeleting};
use crate::workspace::ids::SnapshotId;
use crate::workspace::lifecycle::WorkspaceMountSession;
use crate::workspace::packed_reader_lifecycle::PackedReaderSession;
use crate::workspace::stores::kv_store::packed_admin::{
    PackedCleanAdmission, PackedHeadlessSnapshotDescription, PackedHeadlessSnapshotRequest,
};
use crate::workspace::stores::kv_store::packed_carrier_basis::{
    PackedCarrierBasis, packed_carrier_basis_key, packed_carrier_claim_key,
};
use crate::workspace::stores::kv_store::packed_journal::registry::PackedHistoryRetirementOptions;
use crate::workspace::stores::kv_store::packed_writer_authority::{
    PackedWriterAuthority, PackedWriterOwner, packed_writer_key,
};

const ALIAS_PREFIX: &[u8] = b"packed/borrowed-history/";

pub(super) fn is_creation(writes: &[KvWrite]) -> bool {
    writes
        .iter()
        .any(|write| matches!(write,KvWrite::Put {key,..} if key.starts_with(ALIAS_PREFIX)))
}
pub(super) fn is_logical_retirement(writes: &[KvWrite]) -> bool {
    writes
        .iter()
        .any(|write| matches!(write,KvWrite::Delete {key} if key.starts_with(ALIAS_PREFIX)))
}
pub(super) fn is_carrier_cleanup(writes: &[KvWrite]) -> bool {
    writes.iter().any(|write| {
        matches!(write, KvWrite::Delete { key }
        if key.starts_with(b"packed/sealed-carrier/"))
    })
}

pub(super) fn assert_carrier_cleanup_packet(checks: &[KvCheck], writes: &[KvWrite]) {
    for write in writes {
        let KvWrite::Delete { key } = write else {
            continue;
        };
        if !key.starts_with(b"packed/sealed-carrier/") {
            continue;
        }
        let raw = exact_value(checks, key).as_deref().unwrap();
        let basis = PackedCarrierBasis::decode(raw).unwrap();
        let claim_key = packed_carrier_claim_key(basis.carrier_revision.layer_id);
        assert_eq!(
            exact_value(checks, &claim_key).as_deref(),
            Some(PackedCarrierBasis::claim(raw).unwrap().as_slice())
        );
        assert!(writes.iter().any(|write| matches!(write,
            KvWrite::Delete { key } if key == &claim_key)));
        let layer_key = hot_layer_key(basis.carrier_revision.layer_id);
        let layer: LayerRecord = decode_open_value(
            exact_value(checks, &layer_key).as_deref().unwrap(),
            OPEN_RECORD_MAX_BYTES,
        )
        .unwrap();
        assert_eq!(layer.state, LayerState::Deleting);
        assert!(writes.iter().any(|write| matches!(write,
            KvWrite::Delete { key } if key == &layer_key)));
        assert!(
            checks
                .iter()
                .any(|check| { check.key.as_slice() == CONTROL_KEY && check.expected.is_some() })
        );
        assert!(writes.iter().all(|write| match write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => key.as_slice() != CONTROL_KEY,
        }));
        assert!(writes.iter().any(|write| matches!(write,
            KvWrite::Put { key, .. } if key.as_slice() == TOPOLOGY_GENERATION_KEY)));
        assert_eq!(
            exact_value(
                checks,
                &packed_history_key(
                    basis.source_binding.workspace_id,
                    basis.source_binding.binding.binding_version
                )
            ),
            &None
        );
        assert!(
            checks
                .iter()
                .any(|check| check.key == LAYER_INVENTORY_GENERATION_KEY)
        );
        assert!(
            checks
                .iter()
                .any(|check| check.key == PACKED_ROOT_GENERATION_KEY)
        );
        assert!(writes.iter().all(|write| {
            let key = match write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
            };
            !key.starts_with(b"packed/registry/")
        }));
    }
}
fn put_bytes<'a>(writes: &'a [KvWrite], key: &[u8]) -> &'a [u8] {
    let all = writes
        .iter()
        .filter_map(|write| match write {
            KvWrite::Put { key: actual, value } if actual.as_slice() == key => {
                Some(value.as_slice())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(all.len(), 1);
    all[0]
}
fn exact_value<'a>(checks: &'a [KvCheck], key: &[u8]) -> &'a Option<Vec<u8>> {
    let all = checks
        .iter()
        .filter(|check| check.key.as_slice() == key)
        .collect::<Vec<_>>();
    assert_eq!(all.len(), 1);
    &all[0].expected
}
pub(super) fn assert_creation_packet(checks: &[KvCheck], writes: &[KvWrite]) {
    let child = writes
        .iter()
        .find_map(|write| match write {
            KvWrite::Put { key, value } if key.starts_with(b"packed/current/") => {
                Some(PackedLowerBindingRecord::decode(value).unwrap())
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(child.binding.binding_version, 1);
    assert_eq!(child.head_epoch, 1);
    let descriptor = exact_value(
        checks,
        &packed_carrier_basis_key(child.base_revision.layer_id),
    )
    .clone();
    let claim = exact_value(
        checks,
        &packed_carrier_claim_key(child.base_revision.layer_id),
    )
    .clone();
    let basis = PackedCarrierBasis::decode_pair(&child.base_revision, &descriptor, &claim)
        .unwrap()
        .unwrap();
    assert_ne!(basis.source_binding.workspace_id, child.workspace_id);
    assert_eq!(basis.carrier_revision, child.base_revision);
    assert_eq!(
        basis.source_binding.binding.manifest,
        child.binding.manifest
    );
    assert_eq!(basis.source_binding.highest_inode, child.highest_inode);
    assert_eq!(
        exact_value(
            checks,
            &packed_history_key(
                basis.source_binding.workspace_id,
                basis.source_binding.binding.binding_version
            )
        )
        .as_deref(),
        Some(basis.source_binding.encode().unwrap().as_slice())
    );
    let source_root_key = format!(
        "packed/registry/root/{}",
        basis.registry_incarnation.simple()
    )
    .into_bytes();
    let source_mapping = format!(
        "packed/registry/history-root/{}/{:016x}",
        basis.source_binding.workspace_id, basis.source_binding.binding.binding_version
    )
    .into_bytes();
    assert!(exact_value(checks, &source_root_key).is_some());
    assert_eq!(
        exact_value(checks, &source_root_key),
        exact_value(checks, &source_mapping)
    );
    for key in [
        packed_current_key(child.workspace_id),
        packed_claim_key(child.workspace_id),
        packed_history_key(child.workspace_id, 1),
    ] {
        assert_eq!(*exact_value(checks, &key), None);
    }
    assert_eq!(
        put_bytes(writes, &packed_current_key(child.workspace_id)),
        child.encode().unwrap()
    );
    assert_eq!(
        put_bytes(writes, &packed_history_key(child.workspace_id, 1)),
        child.encode().unwrap()
    );
    assert_eq!(
        put_bytes(writes, &packed_claim_key(child.workspace_id)),
        PACKED_CLAIM
    );
    let head: LayerRecord = decode_open_value(
        put_bytes(writes, &hot_layer_key(child.head_layer_id)),
        OPEN_RECORD_MAX_BYTES,
    )
    .unwrap();
    let workspace: WorkspaceRecord = decode_open_value(
        put_bytes(writes, &hot_workspace_key(child.workspace_id)),
        OPEN_RECORD_MAX_BYTES,
    )
    .unwrap();
    assert_eq!(head.parent_layer_id, Some(child.base_revision.layer_id));
    assert_eq!(head.depth, 2);
    assert_eq!(workspace.fork_base, Some(child.base_revision.clone()));
    assert_eq!(workspace.head_epoch, 1);
    assert!(writes.iter().any(
        |write| matches!(write,KvWrite::Put {key,..} if key.starts_with(b"packed/native-hold/"))
    ));
    for write in writes {
        let key = match write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
        };
        assert!(
            !key.starts_with(b"packed/registry/"),
            "fork must not duplicate graph root or memberships"
        );
    }
}
pub(super) fn assert_logical_retirement_packet(checks: &[KvCheck], writes: &[KvWrite]) {
    let child = writes
        .iter()
        .find_map(|write| match write {
            KvWrite::Delete { key } if key.starts_with(b"packed/history/") => Some(
                PackedLowerBindingRecord::decode(exact_value(checks, key).as_deref().unwrap())
                    .unwrap(),
            ),
            _ => None,
        })
        .expect("logical retirement must delete exactly the child history");
    let writer = PackedWriterAuthority::decode(
        exact_value(checks, &packed_writer_key(child.workspace_id))
            .as_deref()
            .unwrap(),
        child.workspace_id,
    )
    .unwrap();
    assert!(writer.owner.is_none());
    let child_open_key = open_snapshot_key(child.workspace_id);
    let child_open = exact_value(checks, &child_open_key);
    if let Some(raw) = child_open.as_deref() {
        let open: SnapshotOpenRecord = decode_open_value(raw, OPEN_RECORD_MAX_BYTES).unwrap();
        validate_open_record(&open, child.workspace_id).unwrap();
        assert_eq!(open.state, SnapshotOpenState::Ready);
        assert!(!open.recovery_required && open.expires_at_ns > 0);
        assert_eq!(
            writes
                .iter()
                .filter(|write| matches!(write,
            KvWrite::Delete { key } if key == &child_open_key))
                .count(),
            1
        );
    }
    assert!(
        checks
            .iter()
            .any(|check| check.key == PACKED_ROOT_GENERATION_KEY)
    );
    assert!(
        checks
            .iter()
            .any(|check| check.key == LAYER_INVENTORY_GENERATION_KEY)
    );
    for write in writes {
        let key = match write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
        };
        assert!(
            key.starts_with(ALIAS_PREFIX)
                || key.starts_with(b"packed/history/")
                || key.starts_with(b"packed/current/")
                || key.starts_with(b"packed/claim/")
                || key.starts_with(b"packed/borrowed-retirement/")
                || key == PACKED_ROOT_GENERATION_KEY
                || (key == &child_open_key
                    && child_open.is_some()
                    && matches!(write, KvWrite::Delete { .. })),
            "logical alias retirement wrote outside child metadata"
        );
    }
}

pub(super) struct Fixture<'a, B> {
    pub(super) store: &'a Arc<KvWorkspaceStore<FinalDelivery<B>>>,
    pub(super) backend: &'a Arc<FinalDelivery<B>>,
    pub(super) client: ObjectClient<LocalFsBackend>,
    pub(super) budget: Arc<SnapshotMountBudget>,
    pub(super) connect: ForkConnect<B>,
    pub(super) parent_guard: HeadGuard,
    pub(super) target: &'a PackedLowerBindingRecord,
    pub(super) committed: &'a PackedJournalRecord,
    pub(super) native_inode: i64,
    pub(super) native_payload: &'a [u8],
    pub(super) original_lower_payload: &'a [u8],
}

async fn closed_open_retirement_rejections<B: WorkspaceKvBackend>(
    fixture: &Fixture<'_, B>,
    binding: &PackedLowerBindingRecord,
) -> Vec<u8> {
    // Real remote rows are altered with exact CAS only for each negative case,
    // then restored before checking assertions. The retirement call executes
    // its production path against the actual underlying Redis/TiKV backend.
    let backend = fixture.backend.inner.clone();
    let store = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone())
            .with_packed_reader_pin_budget(fixture.budget.clone()),
    );
    let writer_key = packed_writer_key(binding.workspace_id);
    let writer_raw = backend.get(&writer_key).await.unwrap().unwrap();
    let writer = PackedWriterAuthority::decode(&writer_raw, binding.workspace_id).unwrap();
    assert!(writer.owner.is_none());
    let open_key = open_snapshot_key(binding.workspace_id);
    let open_raw = backend.get(&open_key).await.unwrap().unwrap();
    let open: SnapshotOpenRecord = decode_open_value(&open_raw, OPEN_RECORD_MAX_BYTES).unwrap();
    validate_open_record(&open, binding.workspace_id).unwrap();
    assert_eq!(open.state, SnapshotOpenState::Ready);
    assert!(!open.recovery_required && open.expires_at_ns > 0);
    let now = backend.server_time_ns().await.unwrap();
    assert!(open.expires_at_ns <= now);
    let leases = backend.scan_prefix(HOT_LEASE_PREFIX).await.unwrap();
    let (lease_key, lease) = leases
        .iter()
        .find_map(|entry| {
            let lease: SnapshotLease =
                decode_open_value(&entry.value, OPEN_RECORD_MAX_BYTES).unwrap();
            (lease.workspace_id == binding.workspace_id).then(|| (entry.key.clone(), lease))
        })
        .expect("genuine released child mount lease must remain");
    assert_eq!(lease.state, LeaseState::Released);
    let recovery_key = open_snapshot_recovery_key(binding.workspace_id);
    assert!(backend.get(&recovery_key).await.unwrap().is_none());
    type NegativeCatalogMutation = (&'static str, Vec<u8>, Option<Vec<u8>>);
    let mut negative: Vec<NegativeCatalogMutation> = Vec::new();
    negative.push(("missing PWA is not idle", writer_key.clone(), None));
    let mut wrong_writer = writer.clone();
    wrong_writer.workspace_id = WorkspaceId::new();
    negative.push((
        "wrong PWA workspace",
        writer_key.clone(),
        Some(wrong_writer.encode().unwrap()),
    ));
    let mut active_writer = writer.clone();
    active_writer.owner = Some(PackedWriterOwner::InitialSource {
        lease_id: lease.lease_id,
        holder_generation: lease.holder_generation,
    });
    negative.push((
        "retained PWA owner",
        writer_key.clone(),
        Some(active_writer.encode().unwrap()),
    ));
    let mut zero_writer = writer.clone();
    zero_writer.incarnation = 0;
    let mut zero_raw = b"PWA3\x01".to_vec();
    zero_raw.extend(serde_json::to_vec(&zero_writer).unwrap());
    negative.push(("zero PWA incarnation", writer_key.clone(), Some(zero_raw)));
    let mut future_open = open.clone();
    future_open.expires_at_ns = now.checked_add(300_000_000_000).unwrap();
    negative.push((
        "clock before open expiry",
        open_key.clone(),
        Some(encode(&future_open).unwrap()),
    ));
    let mut wrong_open = open.clone();
    wrong_open.workspace_id = WorkspaceId::new();
    negative.push((
        "wrong open workspace",
        open_key.clone(),
        Some(encode(&wrong_open).unwrap()),
    ));
    let mut zero_open = open.clone();
    zero_open.expires_at_ns = 0;
    negative.push((
        "nonpositive open expiry",
        open_key.clone(),
        Some(encode(&zero_open).unwrap()),
    ));
    let mut recovering = open.clone();
    recovering.state = SnapshotOpenState::Recovering;
    recovering.recovery_required = true;
    negative.push((
        "recovering open",
        open_key.clone(),
        Some(encode(&recovering).unwrap()),
    ));
    let mut incomplete = open.clone();
    incomplete.recovery_required = true;
    negative.push((
        "inconsistent Ready recovery flag",
        open_key.clone(),
        Some(encode(&incomplete).unwrap()),
    ));
    negative.push((
        "retained recovery key",
        recovery_key.clone(),
        Some(
            encode(&SnapshotRecoveryRecord {
                workspace_id: binding.workspace_id,
                incomplete: false,
            })
            .unwrap(),
        ),
    ));
    for state in [LeaseState::Active, LeaseState::Expired] {
        let mut retained = lease.clone();
        retained.state = state;
        negative.push((
            if state == LeaseState::Active {
                "nonReleased Active native lease"
            } else {
                "nonReleased Expired native lease"
            },
            lease_key.clone(),
            Some(encode(&retained).unwrap()),
        ));
    }
    let original_namespace = backend.scan_prefix(b"").await.unwrap();
    for (case, key, replacement) in negative {
        let original = backend.get(&key).await.unwrap();
        let inject = match &replacement {
            Some(value) => KvWrite::Put {
                key: key.clone(),
                value: value.clone(),
            },
            None => KvWrite::Delete { key: key.clone() },
        };
        assert!(
            backend
                .compare_and_swap(
                    &[KvCheck {
                        key: key.clone(),
                        expected: original.clone()
                    }],
                    &[inject]
                )
                .await
                .unwrap()
        );
        let before = backend.scan_prefix(b"").await.unwrap();
        let outcome = store
            .clone()
            .retire_borrowed_packed_history(
                binding.clone(),
                fixture.budget.clone(),
                100,
                CancellationToken::new(),
            )
            .await;
        let after = backend.scan_prefix(b"").await.unwrap();
        let restore = match &original {
            Some(value) => KvWrite::Put {
                key: key.clone(),
                value: value.clone(),
            },
            None => KvWrite::Delete { key: key.clone() },
        };
        assert!(
            backend
                .compare_and_swap(
                    &[KvCheck {
                        key,
                        expected: replacement
                    }],
                    &[restore]
                )
                .await
                .unwrap(),
            "negative row was not safely restorable: {case}"
        );
        assert!(
            outcome.is_err(),
            "invalid retirement unexpectedly succeeded: {case}: {outcome:?}"
        );
        assert_eq!(
            after, before,
            "rejected retirement mutated remote metadata: {case}"
        );
        assert_eq!(
            backend.scan_prefix(b"").await.unwrap(),
            original_namespace,
            "negative fixture did not restore exactly: {case}"
        );
    }
    open_raw
}

async fn retired_alias_keeps_later_open<B: WorkspaceKvBackend>(
    fixture: &Fixture<'_, B>,
    binding: &PackedLowerBindingRecord,
    previous_open: &[u8],
) {
    let backend = fixture.backend.inner.clone();
    let key = open_snapshot_key(binding.workspace_id);
    assert!(
        backend.get(&key).await.unwrap().is_none(),
        "first retirement must delete the exact closed open"
    );
    let mut later: SnapshotOpenRecord =
        decode_open_value(previous_open, OPEN_RECORD_MAX_BYTES).unwrap();
    later.generation = later.generation.checked_add(1).unwrap();
    later.expires_at_ns = backend
        .server_time_ns()
        .await
        .unwrap()
        .checked_add(300_000_000_000)
        .unwrap();
    let raw = encode(&later).unwrap();
    assert!(
        backend
            .compare_and_swap(
                &[KvCheck {
                    key: key.clone(),
                    expected: None
                }],
                &[KvWrite::Put {
                    key: key.clone(),
                    value: raw.clone()
                }]
            )
            .await
            .unwrap()
    );
    let before = backend.scan_prefix(b"").await.unwrap();
    let result = fixture
        .store
        .clone()
        .retire_borrowed_packed_history(
            binding.clone(),
            fixture.budget.clone(),
            100,
            CancellationToken::new(),
        )
        .await;
    let after = backend.scan_prefix(b"").await.unwrap();
    assert!(
        backend
            .compare_and_swap(
                &[KvCheck {
                    key: key.clone(),
                    expected: Some(raw.clone())
                }],
                &[KvWrite::Delete { key }]
            )
            .await
            .unwrap(),
        "idempotent retirement deleted or changed the later open"
    );
    let repeated = result.unwrap();
    assert!(repeated.retired);
    assert_eq!(repeated.source_members_released, 0);
    assert_eq!(
        after, before,
        "already-retired branch must be entirely read-only"
    );
}

async fn read_originals<B: WorkspaceKvBackend>(
    fixture: &Fixture<'_, B>,
    binding: &PackedLowerBindingRecord,
    reader: &dyn PackedReaderSession,
) {
    let _owner = reader.retain_request().unwrap();
    let snapshot = AuthenticatedSnapshot::open(&fixture.client, &binding.binding.manifest)
        .await
        .unwrap();
    let index = SnapshotIndexReader::with_budget(fixture.client.clone(), 0, fixture.budget.clone());
    for (name, inode, payload) in [
        (
            b"factory-native".as_slice(),
            u64::try_from(fixture.native_inode).unwrap(),
            fixture.native_payload,
        ),
        (b"nonzero".as_slice(), 400, fixture.original_lower_payload),
    ] {
        let entry = snapshot
            .lookup_dentry(
                &fixture.client,
                &index,
                snapshot.manifest().root_dir_key,
                name,
                1 << 20,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.inode, inode);
        let mut actual = vec![0; payload.len()];
        snapshot
            .read_inode_range(
                &fixture.client,
                &index,
                entry.inode,
                0,
                &mut actual,
                2 << 20,
            )
            .await
            .unwrap();
        assert_eq!(actual, payload);
    }
    reader.validate().await.unwrap();
    index.close().await;
}

async fn repack_child<B: WorkspaceKvBackend>(
    fixture: &Fixture<'_, B>,
    binding: &PackedLowerBindingRecord,
    store: Arc<KvWorkspaceStore<B>>,
    session: WorkspaceMountSession<KvWorkspaceStore<B>>,
    budget: Arc<SnapshotMountBudget>,
) -> (
    PackedLowerBindingRecord,
    HeadGuard,
    WorkspaceMountSession<KvWorkspaceStore<B>>,
    Arc<B>,
    Arc<SnapshotMountBudget>,
) {
    let guard = session.packed_mount_reference().unwrap().guard;
    let scratch = tempfile::tempdir().unwrap();
    let layout = ChunkLayout {
        chunk_size: 4096,
        block_size: 4096,
    };
    let upper = fork_upper(&fixture.client, scratch.path().join("original-cache")).await;
    let snapshot = AuthenticatedSnapshot::open(&fixture.client, &binding.binding.manifest)
        .await
        .unwrap();
    let lower = Arc::new(
        PackedReadonlyMeta::from_snapshot_budget(
            fixture.client.clone(),
            snapshot,
            4096,
            0,
            budget.clone(),
        )
        .unwrap(),
    );
    let reader = store
        .clone()
        .open_packed_reader_session(
            guard.clone(),
            budget.clone(),
            PackedReaderLeaseOptions::default(),
        )
        .await
        .unwrap();
    let meta = Arc::new(
        WorkspaceMetaLayer::with_chunk_size(store.clone(), session.view.clone(), 4096)
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
    meta.chmod(1, 0o777).await.unwrap();
    // Identity installation pins an existing directory before VFS creation,
    // matching the production CLI's writeback-root preparation order.
    let writeback_root = scratch.path().join("original-writeback");
    std::fs::create_dir(&writeback_root).unwrap();
    let config = VFSConfig::new(layout)
        .workspace_writeback_root(writeback_root)
        .workspace_writer_epoch(guard.holder_generation);
    session
        .initialize_packed_writeback_identity(&config)
        .await
        .unwrap();
    let vfs = VFS::from_workspace_components(config, upper, meta).unwrap();
    let released = crate::cli::packed_mount_cutoff::packed_fork_fixture::write_and_release(
        vfs,
        store.clone(),
        session,
        budget,
    )
    .await;
    assert_eq!(released.guard, guard);
    assert!(reader.retain_request().is_err());
    store.shutdown_metadata_backend().await.unwrap();

    // A new administrative runtime has its own real connection and canonical
    // ledger. The independent observer's old pin still protects history1.
    let admin_budget = SnapshotMountBudget::defaults();
    let admin_backend = Arc::new(FinalDelivery::new(Arc::new(
        (fixture.connect)(admin_budget.clone()).await.unwrap(),
    )));
    let admin = Arc::new(
        KvWorkspaceStore::from_arc(admin_backend.clone())
            .with_packed_reader_pin_budget(admin_budget.clone()),
    );
    let ticket = match admin
        .admit_clean_packed_source(released.clone(), admin_budget.clone())
        .await
        .unwrap()
    {
        PackedCleanAdmission::Ready(ticket) => ticket,
        PackedCleanAdmission::RequiresRecovery => panic!("actual child PCR was not admitted"),
    };
    assert_eq!(ticket.released_mount(), released);
    let journal_id = JournalId::new();
    let mut request = PackedHeadlessSnapshotRequest::bounded_operator(
        PackedHeadlessSnapshotDescription {
            snapshot_id: SnapshotId::new(),
            snapshot_name: "actual-fork-child-repack".into(),
            owner_id: None,
        },
        LeaseId::new(),
        journal_id,
        LayerId::new(),
        300_000_000_000,
        scratch.path().join("publication"),
    );
    request.producer = producer_options();
    request.graph_limits = SnapshotIndexAuditLimits::default();
    request.max_rows = 1000;
    request.scratch_disk_bytes = 8 << 20;
    std::fs::create_dir_all(&request.temporary).unwrap();
    let admin_upper = fork_upper(&fixture.client, scratch.path().join("admin-cache")).await;
    admin_backend
        .fork_pause_quiesced
        .store(true, Ordering::SeqCst);
    let publisher = admin.clone();
    let client = fixture.client.clone();
    let task = tokio::spawn(async move {
        publisher
            .publish_clean_packed_snapshot(ticket, client, admin_upper, layout, request)
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        admin_backend.fork_quiesced_entered.acquire(),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    let actual_journal = fixture.store.load_seal_journal(journal_id).await;
    // The actual live native journal cannot be bypassed into a deleting alias.
    let deleting = fixture
        .store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: binding.workspace_id,
            force_fence_lease: true,
        })
        .await;
    let retiring = fixture
        .store
        .clone()
        .retire_borrowed_packed_history(
            binding.clone(),
            fixture.budget.clone(),
            100,
            CancellationToken::new(),
        )
        .await;
    // Release the held response even if a protection assertion fails.
    admin_backend.fork_quiesced_release.add_permits(1);
    let publication = task.await.unwrap();
    let actual_journal = actual_journal.unwrap();
    assert_eq!(actual_journal.phase, SealPhase::Quiesced);
    assert_eq!(actual_journal.workspace_id, binding.workspace_id);
    assert!(deleting.is_err());
    assert!(retiring.is_err());
    let result = match publication {
        Ok(result) => result,
        Err(error) => panic!(
            "actual clean child publication failed: {} committed={}",
            error.error(),
            error.committed_result().is_some(),
        ),
    };
    assert_eq!(result.binding.binding.binding_version, 2);
    assert_ne!(result.binding.base_revision, binding.base_revision);
    let snapshot = AuthenticatedSnapshot::open(&fixture.client, &result.binding.binding.manifest)
        .await
        .unwrap();
    let index = SnapshotIndexReader::with_budget(fixture.client.clone(), 0, fixture.budget.clone());
    let entry = snapshot
        .lookup_dentry(
            &fixture.client,
            &index,
            snapshot.manifest().root_dir_key,
            b"child-repacked",
            1 << 20,
        )
        .await
        .unwrap()
        .unwrap();
    let mut repacked_bytes = [0; 19];
    snapshot
        .read_inode_range(
            &fixture.client,
            &index,
            entry.inode,
            0,
            &mut repacked_bytes,
            2 << 20,
        )
        .await
        .unwrap();
    assert_eq!(&repacked_bytes, b"actual child repack");
    index.close().await;
    assert!(admin_budget.state().closed);
    admin_backend
        .inner
        .shutdown_metadata_backend()
        .await
        .unwrap();

    // Actual publication finishes by releasing its administrative writer. A
    // subsequent real joint mount is required for a current-binding reader.
    let next_budget = SnapshotMountBudget::defaults();
    let next_backend = Arc::new((fixture.connect)(next_budget.clone()).await.unwrap());
    let next_store = Arc::new(
        KvWorkspaceStore::from_arc(next_backend.clone())
            .with_packed_reader_pin_budget(next_budget.clone()),
    );
    let next = WorkspaceMountSession::acquire_for_mount(
        next_store,
        binding.workspace_id,
        guard.holder_generation + 2,
        std::time::Duration::from_secs(300),
        std::time::Duration::from_secs(100),
        false,
        next_budget.clone(),
    )
    .await
    .unwrap();
    let next_guard = next.packed_mount_reference().unwrap().guard;
    assert_eq!(
        next_guard.expected_head_layer_id,
        result.binding.head_layer_id
    );
    assert_eq!(next_guard.expected_head_epoch, result.binding.head_epoch);
    (result.binding, next_guard, next, next_backend, next_budget)
}

async fn fork_upper(
    client: &ObjectClient<LocalFsBackend>,
    cache: std::path::PathBuf,
) -> Arc<ObjectBlockStore<LocalFsBackend>> {
    Arc::new(
        ObjectBlockStore::new_with_configs_async(
            client.clone(),
            ChunksCacheConfig::with_budgets(1 << 20, 1 << 20, cache),
            BlockStoreConfig {
                block_size: 4096,
                compression: Compression::None,
                populate_write_cache_after_upload: false,
                range_background_prefetch: false,
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    )
}

pub(super) async fn contract<B: WorkspaceKvBackend>(fixture: Fixture<'_, B>) {
    let original_graph = fixture
        .backend
        .scan_prefix(b"packed/registry/")
        .await
        .unwrap();
    let marker_keys = [
        packed_carrier_basis_key(fixture.target.base_revision.layer_id),
        packed_carrier_claim_key(fixture.target.base_revision.layer_id),
    ];
    let marker_values = fixture.backend.get_many(&marker_keys).await.unwrap();
    assert!(marker_values.iter().all(Option::is_some));
    assert_eq!(
        fixture
            .store
            .inspect_packed_carrier_revision(&fixture.target.base_revision)
            .await
            .unwrap(),
        *fixture.target
    );
    let topology_before = encode(&test_entity_state(fixture.backend.as_ref()).await).unwrap();
    for remove in [[true, false], [false, true], [true, true]] {
        let checks = marker_keys
            .iter()
            .cloned()
            .zip(marker_values.iter().cloned())
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let writes = marker_keys
            .iter()
            .zip(remove)
            .filter(|(_, remove)| *remove)
            .map(|(key, _)| KvWrite::Delete { key: key.clone() })
            .collect::<Vec<_>>();
        assert!(
            fixture
                .backend
                .inner
                .compare_and_swap(&checks, &writes)
                .await
                .unwrap()
        );
        assert!(
            fixture
                .store
                .inspect_packed_carrier_revision(&fixture.target.base_revision)
                .await
                .is_err()
        );
        assert!(
            fixture
                .store
                .fork_packed_carrier_revision(fixture.target.base_revision.clone(), 1, None)
                .await
                .is_err(),
            "mandatory packed fork cannot downgrade missing carrier markers"
        );
        assert_eq!(
            encode(&test_entity_state(fixture.backend.as_ref()).await).unwrap(),
            topology_before
        );
        assert!(fixture.backend.fork_packets.lock().unwrap().is_empty());
        let restore_checks = marker_keys
            .iter()
            .cloned()
            .zip(marker_values.iter().cloned())
            .zip(remove)
            .map(|((key, value), removed)| KvCheck {
                key,
                expected: if removed { None } else { value },
            })
            .collect::<Vec<_>>();
        let restore_writes = marker_keys
            .iter()
            .cloned()
            .zip(marker_values.iter().cloned())
            .map(|(key, value)| KvWrite::Put {
                key,
                value: value.unwrap(),
            })
            .collect::<Vec<_>>();
        assert!(
            fixture
                .backend
                .inner
                .compare_and_swap(&restore_checks, &restore_writes)
                .await
                .unwrap()
        );
    }
    let children = fixture
        .store
        .fork_packed_carrier_revision(fixture.target.base_revision.clone(), 2, None)
        .await
        .unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(fixture.backend.fork_packets.lock().unwrap().len(), 2);
    assert_eq!(
        fixture
            .backend
            .scan_prefix(b"packed/registry/")
            .await
            .unwrap(),
        original_graph,
        "public fork must enumerate no memberships or change source graph"
    );
    fixture
        .store
        .migrate_native_packed_holds(&fixture.budget, 1000, CancellationToken::new())
        .await
        .unwrap();
    let mut bindings = Vec::new();
    let mut mounts = Vec::new();
    let mut readers = Vec::new();
    for child in &children {
        let binding = fixture
            .store
            .load_packed_binding_version(child.workspace_id, 1)
            .await
            .unwrap()
            .expect("actual public fork lost packed binding");
        assert_eq!(binding.base_revision, fixture.target.base_revision);
        assert_eq!(binding.binding.manifest, fixture.target.binding.manifest);
        let mount_budget = SnapshotMountBudget::defaults();
        let mount_backend = Arc::new((fixture.connect)(mount_budget.clone()).await.unwrap());
        let mount_store = Arc::new(
            KvWorkspaceStore::from_arc(mount_backend)
                .with_packed_reader_pin_budget(mount_budget.clone()),
        );
        let mounted = WorkspaceMountSession::acquire_for_mount(
            mount_store.clone(),
            child.workspace_id,
            1,
            std::time::Duration::from_secs(300),
            std::time::Duration::from_secs(100),
            false,
            mount_budget.clone(),
        )
        .await
        .unwrap();
        let guard = mounted
            .packed_mount_reference()
            .expect("mandatory packed fork mount")
            .guard;
        assert_eq!(guard.expected_head_layer_id, child.head_layer_id);
        assert_eq!(guard.expected_head_epoch, child.head_epoch);
        assert_eq!(
            fixture
                .store
                .load_packed_binding_record(guard.clone())
                .await
                .unwrap(),
            Some(binding.clone())
        );
        let reader = fixture
            .store
            .clone()
            .open_packed_reader_session(
                guard.clone(),
                fixture.budget.clone(),
                PackedReaderLeaseOptions::default(),
            )
            .await
            .unwrap();
        read_originals(&fixture, &binding, reader.as_ref()).await;
        bindings.push(binding);
        mounts.push(Some((mount_store, mounted, mount_budget)));
        readers.push(reader);
    }
    let owner = readers[0].retain_request().unwrap();
    // This first child has never attempted kernel attachment. Its typed abort
    // may retire the joint grant, but cannot mint a clean PCR for repacking.
    let (unattached_store, unattached, unattached_budget) = mounts[0].take().unwrap();
    unattached.release().await.unwrap();
    unattached_store.shutdown_metadata_backend().await.unwrap();
    unattached_budget.close();
    fixture
        .store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: children[0].workspace_id,
            force_fence_lease: true,
        })
        .await
        .unwrap();
    assert!(
        fixture
            .store
            .clone()
            .retire_borrowed_packed_history(
                bindings[0].clone(),
                fixture.budget.clone(),
                100,
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    let draining = readers[0].clone();
    let mut shutdown = Box::pin(draining.shutdown());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(5), &mut shutdown)
            .await
            .is_err()
    );
    assert!(
        fixture
            .store
            .clone()
            .retire_borrowed_packed_history(
                bindings[0].clone(),
                fixture.budget.clone(),
                100,
                CancellationToken::new()
            )
            .await
            .is_err()
    );
    drop(owner);
    shutdown.await.unwrap();
    let retired = fixture
        .store
        .clone()
        .retire_borrowed_packed_history(
            bindings[0].clone(),
            fixture.budget.clone(),
            100,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(retired.retired);
    assert_eq!(retired.source_members_released, 0);
    assert_eq!(
        fixture
            .backend
            .scan_prefix(b"packed/registry/")
            .await
            .unwrap(),
        original_graph
    );
    read_originals(&fixture, &bindings[1], readers[1].as_ref()).await;
    assert!(
        fixture
            .store
            .clone()
            .retire_borrowed_packed_history(
                bindings[1].clone(),
                fixture.budget.clone(),
                100,
                CancellationToken::new()
            )
            .await
            .is_err(),
        "active child history1 remains the birth sentinel"
    );
    let (mounted_store, mounted, mounted_budget) = mounts[1].take().unwrap();
    let (repacked, repacked_guard, repacked_mount, repacked_backend, repacked_budget) =
        repack_child(
            &fixture,
            &bindings[1],
            mounted_store,
            mounted,
            mounted_budget,
        )
        .await;
    assert_eq!(
        fixture
            .store
            .load_packed_binding_record(repacked_guard.clone())
            .await
            .unwrap(),
        Some(repacked.clone())
    );
    let repacked_reader = fixture
        .store
        .clone()
        .open_packed_reader_session(
            repacked_guard,
            fixture.budget.clone(),
            PackedReaderLeaseOptions::default(),
        )
        .await
        .unwrap();
    read_originals(&fixture, &repacked, repacked_reader.as_ref()).await;
    repacked_reader.shutdown().await.unwrap();
    // This post-publication verification session never attempted attachment.
    repacked_mount.release().await.unwrap();
    repacked_backend.shutdown_metadata_backend().await.unwrap();
    repacked_budget.close();
    fixture
        .store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: children[1].workspace_id,
            force_fence_lease: true,
        })
        .await
        .unwrap();
    fixture
        .store
        .mark_workspace_deleting(MarkDeleting {
            workspace_id: fixture.parent_guard.workspace_id,
            force_fence_lease: true,
        })
        .await
        .unwrap();
    let options = || PackedHistoryRetirementOptions {
        incarnation: fixture.committed.source.staging_id,
        grace_ns: 1,
        max_native_holds: 1000,
        max_current_bindings: 1000,
        max_work: u64::MAX,
        cancel: CancellationToken::new(),
    };
    assert!(
        fixture
            .store
            .clone()
            .retire_packed_binding_history(
                fixture.client.clone(),
                fixture.budget.clone(),
                options()
            )
            .await
            .is_err(),
        "Deleting repacked child alias must still retain the old source graph"
    );
    let graph_before = fixture
        .backend
        .scan_prefix(b"packed/registry/")
        .await
        .unwrap();
    assert!(
        fixture
            .store
            .clone()
            .retire_borrowed_packed_history(
                bindings[1].clone(),
                fixture.budget.clone(),
                100,
                CancellationToken::new(),
            )
            .await
            .is_err(),
        "old binding reader must retain its alias after child repack"
    );
    readers[1].shutdown().await.unwrap();
    // Every reader heartbeat has now joined, so full namespace snapshots below
    // attribute any metadata change to this retirement attempt itself.
    let closed_open = closed_open_retirement_rejections(&fixture, &bindings[1]).await;
    let retired = fixture
        .store
        .clone()
        .retire_borrowed_packed_history(
            bindings[1].clone(),
            fixture.budget.clone(),
            100,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(retired.retired);
    assert_eq!(retired.source_members_released, 0);
    retired_alias_keeps_later_open(&fixture, &bindings[1], &closed_open).await;
    assert_eq!(
        fixture
            .backend
            .scan_prefix(b"packed/registry/")
            .await
            .unwrap(),
        graph_before,
        "old child alias retirement must not touch either source object graph"
    );
    assert_eq!(
        fixture
            .backend
            .get(&packed_current_key(repacked.workspace_id))
            .await
            .unwrap(),
        Some(repacked.encode().unwrap())
    );
    assert_eq!(
        fixture
            .backend
            .get(&packed_history_key(repacked.workspace_id, 2))
            .await
            .unwrap(),
        Some(repacked.encode().unwrap())
    );
    assert_eq!(
        fixture
            .backend
            .get(&packed_claim_key(repacked.workspace_id))
            .await
            .unwrap()
            .as_deref(),
        Some(PACKED_CLAIM)
    );
    for binding in &bindings {
        assert_eq!(
            fixture
                .backend
                .get(&packed_history_key(binding.workspace_id, 1))
                .await
                .unwrap(),
            None
        );
        let repeated = fixture
            .store
            .clone()
            .retire_borrowed_packed_history(
                binding.clone(),
                fixture.budget.clone(),
                100,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(repeated.retired);
        assert_eq!(repeated.source_members_released, 0);
    }
    assert_eq!(
        fixture
            .backend
            .alias_retirement_packets
            .lock()
            .unwrap()
            .len(),
        2
    );
    let observed = fixture
        .store
        .clone()
        .retire_packed_binding_history(fixture.client.clone(), fixture.budget.clone(), options())
        .await
        .unwrap();
    assert!(observed.observing && !observed.retired);
    let finished = fixture
        .store
        .clone()
        .retire_packed_binding_history(fixture.client.clone(), fixture.budget.clone(), options())
        .await
        .unwrap();
    assert!(finished.retired);
    let graph_terminal = fixture
        .backend
        .scan_prefix(b"packed/registry/")
        .await
        .unwrap();
    let carrier = fixture.target.base_revision.layer_id;
    fixture
        .store
        .delete_layer_metadata(DeleteLayerMetadata {
            layer_ids: vec![carrier],
            now_ns: fixture.backend.server_time_ns().await.unwrap(),
            lease_grace_ns: 0,
        })
        .await
        .unwrap();
    fixture
        .store
        .finalize_layer_metadata_deletion(vec![carrier])
        .await
        .unwrap();
    assert_eq!(
        fixture.backend.carrier_cleanup_calls.load(Ordering::SeqCst),
        1
    );
    for key in [
        hot_layer_key(carrier),
        packed_carrier_basis_key(carrier),
        packed_carrier_claim_key(carrier),
    ] {
        assert_eq!(fixture.backend.get(&key).await.unwrap(), None);
    }
    assert_eq!(
        fixture
            .backend
            .scan_prefix(b"packed/registry/")
            .await
            .unwrap(),
        graph_terminal
    );
    assert_eq!(
        fixture
            .backend
            .get(&packed_current_key(repacked.workspace_id))
            .await
            .unwrap(),
        Some(repacked.encode().unwrap())
    );
    fixture
        .store
        .finalize_layer_metadata_deletion(vec![carrier])
        .await
        .unwrap();
    assert_eq!(
        fixture.backend.carrier_cleanup_calls.load(Ordering::SeqCst),
        1
    );
}

#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_REDIS_URL and Linux FUSE; actual joint mount/PCR/repack/retirement chain"]
async fn real_redis_native_carrier_fork_alias_siblings_pins_journals_and_retirement() {
    redis(Delivery::ForkAliases).await;
}
#[tokio::test]
#[ignore = "requires isolated BREWFS_TEST_TIKV_PD_ENDPOINTS and Linux FUSE; actual joint mount/PCR/repack/retirement chain"]
async fn real_tikv_native_carrier_fork_alias_siblings_pins_journals_and_retirement() {
    tikv(Delivery::ForkAliases).await;
}
