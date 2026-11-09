//! Actual OS process death at durable Prepare/Q/PNB, then another process.
//! Packed payloads live in LocalFS. Six cases add only actual KV upper metadata;
//! two Prepare cases also persist fresh VFS upper payload in LocalFsBlockStore.

use super::*;
use crate::cadapter::localfs::LocalFsBackend;
use crate::chunk::cache::ChunksCacheConfig;
use crate::chunk::compress::Compression;
use crate::chunk::store::{BlockStoreConfig, LocalFsBlockStore};
use crate::meta::store::AclRule;
use crate::vfs::cache::config::WriteBackMode;
use crate::workspace_overlay::ids::{LeaseId, WorkspaceId};
use crate::workspace_overlay::model::ExtentKind;
use crate::workspace_overlay::packed_reader_lifecycle::{
    KvPackedReaderSession, PackedReaderSession,
};
use crate::workspace_overlay::stores::binding_tests::packed_for_process_restart;
use crate::workspace_overlay::stores::kv_store::packed_native_freeze::{
    NativePackedRecoveryClaimRequest, NativePrepareRecoveryRequest,
};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

const CHILD_WAIT: Duration = Duration::from_secs(180);
const EXIT_WAIT: Duration = Duration::from_secs(15);
const CLEANUP_WAIT: Duration = Duration::from_secs(30);
const STATE_BYTES: u64 = 128 << 10;
const XATTR: &str = "user.actual-process-restart";
const XATTR_VALUE: &[u8] = b"KV metadata survives an actual killed process";

type ProcessMeta<B> = WorkspaceMetaLayer<KvWorkspaceStore<B>>;
type ProcessVfs<B, S> = VFS<S, ProcessMeta<B>>;
type ChildTerminals = Arc<std::sync::Mutex<Vec<Arc<ChildTerminal>>>>;

struct ChildTerminal {
    pid: u32,
    terminal: AtomicBool,
}

async fn point<B: WorkspaceKvBackend>(backend: &B, key: &[u8], bound: usize) -> Option<Vec<u8>> {
    let (mut rows, _) = backend
        .get_many_consistent_with_time_bounded(
            &[key.to_vec()],
            KvReadLimits {
                max_records: 1,
                max_key_bytes: 512,
                max_value_bytes: bound,
                max_total_bytes: bound.checked_add(key.len()).unwrap(),
                max_response_bytes: 64 << 10,
                max_data_requests: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "bounded diagnostic point result length");
    rows.pop().unwrap()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Boundary {
    Prepare,
    Quiesced,
    Pnb,
}
impl Boundary {
    fn spelling(self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::Quiesced => "quiesced",
            Self::Pnb => "pnb",
        }
    }
    fn parse(value: &str) -> Self {
        match value {
            "prepare" => Self::Prepare,
            "quiesced" => Self::Quiesced,
            "pnb" => Self::Pnb,
            _ => panic!("invalid process restart boundary"),
        }
    }
}

// Diagnostic facts only. They have no conversion to a recovery/read/hash token.
#[derive(Serialize, Deserialize)]
struct RestartFacts {
    schema: u32,
    boundary: Boundary,
    staged_pid: u32,
    fixture_dir: String,
    workspace: WorkspaceId,
    old_head: LayerId,
    old_epoch: u64,
    old_lease: LeaseId,
    old_generation: u64,
    native_journal: JournalId,
    planned_head: LayerId,
    lease_expiry: i64,
    binding: Vec<u8>,
    upper_delta_digest: [u8; 32],
    upper_payload: Option<UpperPayloadFacts>,
    original_q: Option<Vec<u8>>,
    ppj: Option<JournalId>,
    ppj_bytes: Option<Vec<u8>>,
    native_hash: Option<([u8; 32], [u8; 32])>,
}
impl RestartFacts {
    fn original_guard(&self) -> HeadGuard {
        HeadGuard {
            workspace_id: self.workspace,
            expected_head_layer_id: self.old_head,
            expected_head_epoch: self.old_epoch,
            lease_id: self.old_lease,
            holder_generation: self.old_generation,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RestartSuccess {
    schema: u32,
    staged_pid: u32,
    recovered_pid: u32,
    boundary: Boundary,
    old_lease: LeaseId,
    current_lease: LeaseId,
    current_generation: u64,
    target: Vec<u8>,
    committed_ppj: JournalId,
    upper_inode: Option<i64>,
}

#[derive(Serialize, Deserialize)]
struct UpperPayloadFacts {
    inode: i64,
    slice_ids: Vec<u64>,
}

fn upper_bytes() -> Vec<u8> {
    (0..4096).map(|i| (i % 239 + 3) as u8).collect()
}

async fn persistent_upper(root: &Path, phase: &str) -> Arc<LocalFsBlockStore> {
    assert!(matches!(phase, "stage" | "recover"));
    Arc::new(
        LocalFsBlockStore::new_with_configs_async(
            ObjectClient::new(LocalFsBackend::new(root.join("upper-objects"))),
            ChunksCacheConfig {
                hot_cache_size: 16,
                cold_cache_size: 16,
                ..ChunksCacheConfig::with_budgets(0, 0, root.join(format!("upper-cache-{phase}")))
            },
            BlockStoreConfig {
                block_size: 4096,
                page_size: 4096,
                page_cache_capacity: 0,
                range_read_threshold: 1.0,
                range_background_prefetch: false,
                compression: Compression::None,
                populate_write_cache_after_upload: false,
                persist_write_cache_after_upload: false,
                persistent_slice_cache_dir: None,
                create_only_writes: true,
                versioned_objects_only: true,
                versioned_objects_uncompressed: true,
                write_coalesce_bytes: 0,
                write_coalesce_ms: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    )
}

fn assert_persistent_blocks(root: &Path, facts: &UpperPayloadFacts) {
    assert!(facts.inode > 400);
    assert!(!facts.slice_ids.is_empty());
    assert!(facts.slice_ids.len() <= 4);
    let root = root.canonicalize().unwrap();
    for &slice in &facts.slice_ids {
        assert_ne!(slice, 0);
        let path = root
            .join(format!("upper-objects/chunks-v2/{slice}/0"))
            .canonicalize()
            .unwrap();
        assert!(path.starts_with(root.join("upper-objects")));
        let length = std::fs::metadata(path).unwrap().len();
        assert_eq!(
            length,
            4096 + crate::chunk::compress::PERSISTED_HEADER_LEN as u64
        );
    }
}

struct SourceMount<'a> {
    guard: &'a HeadGuard,
    binding: &'a PackedLowerBindingRecord,
    client: &'a ObjectClient<LocalFsBackend>,
    budget: Arc<V3MountBudget>,
    initialize: bool,
    persistent_upper: bool,
}

fn write_state<T: Serialize>(root: &Path, name: &str, value: &T) {
    let bytes = serde_json::to_vec(value).unwrap();
    assert!(bytes.len() as u64 <= STATE_BYTES);
    let pending = root.join(format!("{name}.pending"));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending)
        .unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    drop(file);
    std::fs::rename(pending, root.join(name)).unwrap();
    File::open(root).unwrap().sync_all().unwrap();
}

fn read_state<T: for<'a> Deserialize<'a>>(root: &Path, name: &str) -> T {
    let file = File::open(root.join(name)).unwrap();
    assert!(file.metadata().unwrap().len() <= STATE_BYTES);
    let mut bytes = Vec::new();
    file.take(STATE_BYTES + 1).read_to_end(&mut bytes).unwrap();
    assert!(bytes.len() as u64 <= STATE_BYTES);
    serde_json::from_slice(&bytes).unwrap()
}

fn fixture_path(root: &Path, relative: &str) -> PathBuf {
    let relative = Path::new(relative);
    assert_eq!(relative.components().count(), 1);
    assert!(matches!(
        relative.components().next(),
        Some(Component::Normal(_))
    ));
    let path = root.join(relative).canonicalize().unwrap();
    assert_eq!(path.parent(), Some(root));
    path
}

fn acl() -> AclRule {
    AclRule {
        acl_type: 1,
        qualifier: 42,
        permissions: 5,
    }
}

fn layout() -> ChunkLayout {
    ChunkLayout {
        chunk_size: 4096,
        block_size: 4096,
    }
}

fn build_options(scratch: &Path) -> NativePublicationBuildOptions {
    NativePublicationBuildOptions {
        producer: producer_options(),
        temporary: scratch.to_path_buf(),
        graph_scratch: scratch.to_path_buf(),
        graph_limits: V3IndexAuditLimits::default(),
        native_hash_limits: NativeDeltaHashLimits {
            max_native_delta_rows: 1000,
            max_canonical_bytes: 8 << 20,
        },
        chunk_size: 4096,
        metadata_cache_bytes: 0,
        max_catalog_rows: 100,
        cancel: CancellationToken::new(),
    }
}

async fn source_vfs<B: WorkspaceKvBackend, S: BlockStore + Send + Sync + 'static>(
    store: Arc<KvWorkspaceStore<B>>,
    reader: Arc<dyn PackedReaderSession>,
    upper: Arc<S>,
    mount: SourceMount<'_>,
) -> (ProcessVfs<B, S>, Arc<ProcessMeta<B>>) {
    let SourceMount {
        guard,
        binding,
        client,
        budget,
        initialize,
        persistent_upper,
    } = mount;
    let snapshot = AuthenticatedV3Snapshot::open(client, &binding.binding.manifest)
        .await
        .unwrap();
    let lower = Arc::new(
        PackedV3ReadonlyMeta::from_v3_budget(client.clone(), snapshot, 4096, 0, budget).unwrap(),
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
        .with_packed_v3_lower(
            binding.binding.clone(),
            lower,
            Arc::new(PinnedCatalogPackedBindingAuthority { store, reader }),
            upper.clone(),
            layout(),
        )
        .unwrap(),
    );
    if initialize {
        meta.initialize().await.unwrap();
    }
    let provider: Arc<dyn WorkspaceReadPlanProvider> = meta.clone();
    let config = VFSConfig::new(layout());
    let config = if persistent_upper {
        let write = (*config.write)
            .clone()
            .writeback_mode(WriteBackMode::UploadBeforeCommit);
        config.write_config(write)
    } else {
        config
    };
    let vfs =
        VFS::from_readonly_components_with_provider(config, upper, meta.clone(), provider).unwrap();
    (vfs, meta)
}

async fn stage<B: WorkspaceKvBackend, S: BlockStore + Send + Sync + 'static>(
    backend: Arc<B>,
    root: &Path,
    boundary: Boundary,
    upper: Arc<S>,
    persist_upper: bool,
) {
    if persist_upper {
        assert_eq!(boundary, Boundary::Prepare);
    }
    let (objects, client, _snapshot, proof, _) = packed_for_process_restart(root).await;
    let object_dir = objects.keep();
    let fixture_dir = object_dir.file_name().unwrap().to_str().unwrap().to_owned();
    assert_eq!(
        fixture_path(root, &fixture_dir),
        object_dir.canonicalize().unwrap()
    );
    let scratch = root.join("stage-scratch");
    std::fs::create_dir(&scratch).unwrap();
    let backend = Arc::new(FinalDelivery::new(backend));
    let store = Arc::new(KvWorkspaceStore::from_arc(backend.clone()));
    let install = request(store.as_ref(), proof).await;
    let binding = store
        .install_packed_lower_binding(install.clone())
        .await
        .unwrap();
    let guard = HeadGuard {
        expected_head_epoch: binding.head_epoch,
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
            V3MountBudget::defaults(),
            PackedReaderLeaseOptions::default(),
        )
        .await
        .unwrap();
    let budget = reader.mount_budget();
    let (vfs, meta) = source_vfs(
        store.clone(),
        reader.clone(),
        upper,
        SourceMount {
            guard: &guard,
            binding: &binding,
            client: &client,
            budget: budget.clone(),
            initialize: true,
            persistent_upper: persist_upper,
        },
    )
    .await;
    let linked = vfs.link("/nonzero", "/alias").await.unwrap();
    assert_eq!(linked.ino, 400);
    assert_eq!(linked.nlink, 2);
    vfs.set_xattr_ino(400, XATTR, XATTR_VALUE, 0).await.unwrap();
    vfs.set_acl_ino(400, acl()).await.unwrap();
    assert_eq!(
        vfs.get_xattr_ino(400, XATTR).await.unwrap(),
        Some(XATTR_VALUE.to_vec())
    );
    assert_eq!(vfs.get_acl_ino(400, 1, 42).await.unwrap(), Some(acl()));
    let upper_inode = if persist_upper {
        let inode = vfs.create_file("/upper-process").await.unwrap();
        let handle = vfs
            .open(
                inode,
                vfs.stat("/upper-process").await.unwrap(),
                true,
                true,
                false,
            )
            .await
            .unwrap();
        let payload = upper_bytes();
        assert_eq!(vfs.write(handle, 0, &payload).await.unwrap(), payload.len());
        vfs.flush(handle).await.unwrap();
        vfs.close(handle).await.unwrap();
        let linked = vfs.link("/upper-process", "/upper-alias").await.unwrap();
        assert_eq!(linked.ino, inode);
        assert_eq!(linked.nlink, 2);
        Some(inode)
    } else {
        None
    };
    let mut local = Some(vfs.quiesce_packed_vfs().await.unwrap());
    let upper_delta = store
        .load_layer_delta(guard.expected_head_layer_id)
        .await
        .unwrap();
    let upper_payload = if let Some(inode) = upper_inode {
        let data = upper_delta
            .extents
            .iter()
            .filter(|row| row.ino == inode)
            .collect::<Vec<_>>();
        assert!(!data.is_empty(), "actual VFS upper Data extents absent");
        assert!(data.len() <= 4);
        let mut slices = Vec::new();
        for row in data {
            assert_eq!(row.chunk_index, 0);
            assert_eq!(row.logical_offset, 0);
            assert_eq!(row.length, 4096);
            let ExtentKind::Data {
                slice_id,
                slice_offset,
            } = row.kind
            else {
                panic!("upper VFS payload became a hole")
            };
            assert_eq!(slice_offset, 0);
            slices.push(slice_id);
        }
        slices.sort_unstable();
        slices.dedup();
        let facts = UpperPayloadFacts {
            inode,
            slice_ids: slices,
        };
        assert_persistent_blocks(root, &facts);
        Some(facts)
    } else {
        assert!(
            upper_delta.extents.is_empty(),
            "test wrote volatile upper payload"
        );
        None
    };
    let layers: [LayerRecord; 2] = store
        .load_layer_chain(guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let native_journal = JournalId::new();
    let planned_head = LayerId::new();
    let native = if boundary == Boundary::Prepare {
        backend.stop_original_seed_q.store(true, Ordering::SeqCst);
        assert!(
            store
                .clone()
                .begin_packed_native_quiesce(
                    guard.clone(),
                    layers,
                    native_journal,
                    planned_head,
                    budget.clone(),
                )
                .await
                .is_err()
        );
        let control = test_entity_state(backend.as_ref()).await;
        assert_eq!(control.journals[&native_journal].phase, SealPhase::Prepare);
        None
    } else {
        Some(Arc::new(
            store
                .clone()
                .begin_packed_native_quiesce(
                    guard.clone(),
                    layers,
                    native_journal,
                    planned_head,
                    budget.clone(),
                )
                .await
                .unwrap(),
        ))
    };
    let mut facts = RestartFacts {
        schema: 1,
        boundary,
        staged_pid: std::process::id(),
        fixture_dir,
        workspace: guard.workspace_id,
        old_head: guard.expected_head_layer_id,
        old_epoch: guard.expected_head_epoch,
        old_lease: guard.lease_id,
        old_generation: guard.holder_generation,
        native_journal,
        planned_head,
        lease_expiry: 0,
        binding: binding.encode().unwrap(),
        upper_delta_digest: delta_digest(&upper_delta).unwrap(),
        upper_payload,
        original_q: native
            .as_ref()
            .map(|fence| fence.canonical_receipt_bytes().to_vec()),
        ppj: None,
        ppj_bytes: None,
        native_hash: None,
    };
    let ready = if boundary == Boundary::Pnb {
        let artifact = FrozenNativeArtifact::capture(
            native.as_ref().unwrap().clone(),
            local.take().unwrap(),
            scratch.clone(),
            capture_limits(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let ready = match store
            .prepare_frozen_native_publication(artifact, client.clone(), build_options(&scratch))
            .await
        {
            Ok(ready) => ready,
            Err(error) => panic!(
                "process staging factory failed: {}",
                preparation_error(&error)
            ),
        };
        assert_eq!(ready.record.phase, PackedJournalPhase::Verified);
        assert_eq!(
            ready.source.phase_authority().canonical_receipt_bytes(),
            facts.original_q.as_ref().unwrap()
        );
        facts.ppj = Some(ready.record.journal_id);
        facts.ppj_bytes = Some(ready.record.value.encode().unwrap());
        facts.native_hash = Some((
            ready
                .source
                .phase_authority()
                .native_delta_hash()
                .delta_digest(),
            ready
                .source
                .phase_authority()
                .native_delta_hash()
                .root_hash(),
        ));
        Some(ready)
    } else {
        None
    };
    let expired = store
        .renew_lease(RenewLease {
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
            ttl_ns: 500_000_000,
        })
        .await
        .unwrap();
    facts.lease_expiry = expired.expires_at_ns;
    write_state(root, "staged.json", &facts);
    // Parent kills this exact child while all old opaque/drain/hash/reader
    // authorities are alive. There is no shutdown/drop handoff to recovery.
    std::future::pending::<()>().await;
    drop((ready, native, local, vfs, meta, reader, store, budget));
}

async fn verify_published(
    client: ObjectClient<LocalFsBackend>,
    target: &PackedLowerBindingRecord,
    budget: Arc<V3MountBudget>,
    upper: Option<&UpperPayloadFacts>,
) {
    let snapshot = AuthenticatedV3Snapshot::open(&client, &target.binding.manifest)
        .await
        .unwrap();
    let candidate =
        PackedV3ReadonlyMeta::from_v3_budget(client, snapshot, 4096, 0, budget).unwrap();
    assert_eq!(candidate.lookup(1, "nonzero").await.unwrap(), Some(400));
    assert_eq!(candidate.lookup(1, "alias").await.unwrap(), Some(400));
    let attr = candidate.stat(400).await.unwrap().unwrap();
    assert_eq!(attr.nlink, 2);
    assert_eq!(attr.size, 4096);
    assert_eq!(
        candidate.get_xattr(400, XATTR).await.unwrap(),
        Some(XATTR_VALUE.to_vec())
    );
    assert_eq!(candidate.get_acl(400, 1, 42).await.unwrap(), Some(acl()));
    let expected: Vec<u8> = (0..4096).map(|i| (i % 251 + 1) as u8).collect();
    let prepared = candidate
        .prepare_unified_read(400, 0, 0, 4096)
        .await
        .unwrap()
        .unwrap();
    let mut actual = vec![0; expected.len()];
    execute_unified_into(prepared.fetcher.as_ref(), 0, &prepared.plan, &mut actual)
        .await
        .unwrap();
    assert_eq!(
        actual, expected,
        "LocalFS payload changed across real process death"
    );
    drop(prepared);
    if let Some(upper) = upper {
        assert_eq!(
            candidate.lookup(1, "upper-process").await.unwrap(),
            Some(upper.inode)
        );
        assert_eq!(
            candidate.lookup(1, "upper-alias").await.unwrap(),
            Some(upper.inode)
        );
        let attr = candidate.stat(upper.inode).await.unwrap().unwrap();
        assert_eq!(attr.nlink, 2);
        assert_eq!(attr.size, 4096);
        let expected = upper_bytes();
        let prepared = candidate
            .prepare_unified_read(upper.inode, 0, 0, 4096)
            .await
            .unwrap()
            .unwrap();
        let mut actual = vec![0; expected.len()];
        execute_unified_into(prepared.fetcher.as_ref(), 0, &prepared.plan, &mut actual)
            .await
            .unwrap();
        assert_eq!(
            actual, expected,
            "new VFS upper payload did not survive independent process restart"
        );
    }
}

async fn recover<B: WorkspaceKvBackend, S: BlockStore + Send + Sync + 'static>(
    backend: Arc<B>,
    root: &Path,
    boundary: Boundary,
    upper: Arc<S>,
    persist_upper: bool,
) {
    let facts: RestartFacts = read_state(root, "staged.json");
    assert_eq!(facts.schema, 1);
    assert_eq!(facts.boundary, boundary);
    assert_eq!(facts.upper_payload.is_some(), persist_upper);
    if persist_upper {
        assert_eq!(boundary, Boundary::Prepare);
    }
    assert_ne!(facts.staged_pid, std::process::id());
    let fixture = fixture_path(root, &facts.fixture_dir);
    let client = ObjectClient::new(LocalFsBackend::new(fixture.join("objects")));
    let binding = PackedLowerBindingRecord::decode(&facts.binding).unwrap();
    let old_guard = facts.original_guard();
    let store = Arc::new(KvWorkspaceStore::from_arc(backend.clone()));
    let budget = V3MountBudget::defaults();
    let owner = format!("process-restarted-{}", Uuid::new_v4());
    let new_lease = LeaseId::new();
    let open = store
        .open_workspace_v3(facts.workspace, owner.clone(), Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(open.state, V3OpenState::Recovering);
    assert!(open.recovery_required);
    assert_eq!(
        PackedLowerBindingRecord::decode(
            &point(
                backend.as_ref(),
                &packed_current_key(facts.workspace),
                48 << 10
            )
            .await
            .unwrap()
        )
        .unwrap(),
        binding
    );
    tokio::time::timeout(CLEANUP_WAIT, async {
        while backend.server_time_ns().await.unwrap() < facts.lease_expiry {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("real backend lease expiry after process death");
    let scratch = root.join("recovery-scratch");
    std::fs::create_dir(&scratch).unwrap();
    let recovery = if let Some(ppj) = facts.ppj {
        let bytes = point(backend.as_ref(), &journal_key(ppj), 48 << 10)
            .await
            .unwrap();
        assert_eq!(Some(&bytes), facts.ppj_bytes.as_ref());
        let record = PackedJournalRecord::decode(&bytes).unwrap();
        let basis = store
            .inspect_native_packed_recovery_basis(&record, &budget)
            .await
            .unwrap();
        let claim = store
            .claim_native_packed_recovery(
                basis,
                NativePackedRecoveryClaimRequest {
                    new_lease_id: new_lease,
                    owner_id: owner.clone(),
                    ttl_ns: 300_000_000_000,
                },
                budget.clone(),
            )
            .await
            .unwrap();
        let basis = store
            .inspect_native_packed_recovery_basis(&record, &budget)
            .await
            .unwrap();
        Some((
            record,
            store
                .reissue_native_source_read_claimed(basis, claim, budget.clone())
                .await
                .unwrap(),
        ))
    } else {
        None
    };
    let native = if let Some((_, recovery)) = &recovery {
        recovery.native_quiesce().clone()
    } else {
        store
            .recover_packed_native_prepare(
                NativePrepareRecoveryRequest {
                    journal_id: facts.native_journal,
                    owner_id: owner,
                    new_lease_id: Some(new_lease),
                    ttl_ns: 300_000_000_000,
                },
                budget.clone(),
            )
            .await
            .unwrap()
    };
    let source_guard = native.source_guard().clone();
    assert_eq!(native.mapping().old_guard(), &old_guard);
    assert_eq!(native.mapping().journal_id(), facts.native_journal);
    assert_eq!(native.mapping().planned_head_layer_id(), facts.planned_head);
    assert_eq!(source_guard.lease_id, new_lease);
    assert_eq!(source_guard.holder_generation, facts.old_generation + 1);
    if let Some(original_q) = &facts.original_q {
        assert_eq!(native.canonical_receipt_bytes(), original_q);
    }
    let reader: Arc<dyn PackedReaderSession> = if let Some((_, recovery)) = &recovery {
        KvPackedReaderSession::open_native_recovery(
            recovery.clone(),
            budget.clone(),
            PackedReaderLeaseOptions::default(),
        )
        .await
        .unwrap()
    } else {
        KvPackedReaderSession::open_native_prepare(
            &store,
            native.clone(),
            budget.clone(),
            PackedReaderLeaseOptions::default(),
        )
        .await
        .unwrap()
    };
    reader.validate().await.unwrap();
    let (vfs, meta) = source_vfs(
        store.clone(),
        reader.clone(),
        upper.clone(),
        SourceMount {
            guard: &source_guard,
            binding: &binding,
            client: &client,
            budget: budget.clone(),
            initialize: false,
            persistent_upper: persist_upper,
        },
    )
    .await;
    // Ordinary mixed reads require a Writable head. These durable metadata
    // rows are facts only; capture below reads through the new typed Sealing
    // authority, and the independently opened published candidate is checked.
    let upper_delta = store.load_layer_delta(facts.old_head).await.unwrap();
    assert_eq!(
        delta_digest(&upper_delta).unwrap(),
        facts.upper_delta_digest
    );
    if let Some(payload) = &facts.upper_payload {
        assert_persistent_blocks(root, payload);
        assert!(
            upper_delta
                .extents
                .iter()
                .any(|row| row.ino == payload.inode && matches!(row.kind, ExtentKind::Data { .. }))
        );
        let expected = upper_bytes();
        for &slice in &payload.slice_ids {
            let mut actual = vec![0; expected.len()];
            upper.read_range((slice, 0), 0, &mut actual).await.unwrap();
            assert_eq!(
                actual, expected,
                "fresh LocalFsBlockStore could not reopen actual committed upper bytes"
            );
        }
    } else {
        assert!(upper_delta.extents.is_empty());
    }
    assert!(
        upper_delta
            .dentries
            .iter()
            .any(|row| row.name == b"alias" && row.ino == Some(400))
    );
    assert!(
        upper_delta
            .inodes
            .iter()
            .any(|row| row.ino == 400 && row.nlink == 2)
    );
    assert!(upper_delta.xattrs.iter().any(|row| row.ino == 400
        && row.name == XATTR.as_bytes()
        && row.value.as_deref() == Some(XATTR_VALUE)));
    assert!(
        upper_delta.acls.iter().any(|row| row.ino == 400
            && row.acl_type == 1
            && row.acl_id == 42
            && row.value.is_some())
    );
    let local = vfs.quiesce_packed_vfs().await.unwrap();
    let artifact = FrozenNativeArtifact::capture(
        native.clone(),
        local,
        scratch.clone(),
        capture_limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let ready = if let Some((record, recovery)) = recovery {
        assert_eq!(
            artifact.source_digest().unwrap(),
            record.source.effective_view_digest
        );
        let target = record.commit_target.as_ref().unwrap();
        let snapshot = AuthenticatedV3Snapshot::open(&client, &target.binding.manifest)
            .await
            .unwrap();
        let candidate = Arc::new(
            PackedV3ReadonlyMeta::from_v3_budget(client.clone(), snapshot, 4096, 0, budget.clone())
                .unwrap(),
        );
        let source = artifact
            .compare_recovery_candidate(recovery, candidate)
            .await
            .unwrap();
        let hash = Arc::new(
            FrozenNativeDeltaHash::capture(
                source.native_quiesce().clone(),
                source.native_reader_session(),
                build_options(&scratch).native_hash_limits,
            )
            .await
            .unwrap(),
        );
        assert_eq!(
            Some((hash.delta_digest(), hash.root_hash())),
            facts.native_hash
        );
        let source = match source.promote_native_hashed(hash).await.unwrap() {
            Ok(source) => source,
            Err(error) => panic!("new process Hashed reissue failed: {}", error.error),
        };
        let graph = store
            .audit_hashed_native_effective_graph(
                &record,
                &source,
                &client,
                &budget,
                ImportedGraphAuditOptions {
                    scratch: &scratch,
                    limits: V3IndexAuditLimits::default(),
                    cancel: CancellationToken::new(),
                },
            )
            .await
            .unwrap();
        match store
            .assemble_native_publication(source, graph, budget.clone())
            .await
        {
            Ok(ready) => ready,
            Err(error) => panic!("new process assembly failed: {}", preparation_error(&error)),
        }
    } else {
        match store
            .prepare_frozen_native_publication(artifact, client.clone(), build_options(&scratch))
            .await
        {
            Ok(ready) => ready,
            Err(error) => panic!(
                "new process Prepare/Q factory failed: {}",
                preparation_error(&error)
            ),
        }
    };
    assert_eq!(ready.record.guard, old_guard);
    assert_eq!(ready.source.phase_authority().source_guard(), &source_guard);
    let ppj = ready.record.journal_id;
    let target = ready.record.commit_target.clone().unwrap();
    let outcome = match ready.commit().await {
        Ok(outcome) => outcome,
        Err(error) => panic!("new process final CAS failed: {}", error.error),
    };
    let published_guard = HeadGuard {
        expected_head_layer_id: target.head_layer_id,
        expected_head_epoch: target.head_epoch,
        ..source_guard.clone()
    };
    assert_eq!(outcome.guard, published_guard);
    assert_eq!(
        store
            .load_packed_binding_record(published_guard)
            .await
            .unwrap(),
        Some(target.clone())
    );
    assert_eq!(
        PackedJournalRecord::decode(
            &point(backend.as_ref(), &journal_key(ppj), 48 << 10)
                .await
                .unwrap()
        )
        .unwrap()
        .phase,
        PackedJournalPhase::Committed
    );
    let ready_open: V3OpenRecord = decode_open_value(
        &point(
            backend.as_ref(),
            &open_v3_key(facts.workspace),
            OPEN_RECORD_MAX_BYTES,
        )
        .await
        .unwrap(),
        OPEN_RECORD_MAX_BYTES,
    )
    .unwrap();
    assert_eq!(ready_open.state, V3OpenState::Ready);
    assert!(!ready_open.recovery_required);
    verify_published(
        client,
        &target,
        budget.clone(),
        facts.upper_payload.as_ref(),
    )
    .await;
    drop((outcome, native, vfs, meta));
    reader.shutdown().await.unwrap();
    write_state(
        root,
        "recovered.json",
        &RestartSuccess {
            schema: 1,
            staged_pid: facts.staged_pid,
            recovered_pid: std::process::id(),
            boundary,
            old_lease: facts.old_lease,
            current_lease: source_guard.lease_id,
            current_generation: source_guard.holder_generation,
            target: target.encode().unwrap(),
            committed_ppj: ppj,
            upper_inode: facts.upper_payload.as_ref().map(|upper| upper.inode),
        },
    );
    backend.shutdown_metadata_backend().await.unwrap();
}

struct OwnedChild {
    child: Child,
    receipt: Arc<ChildTerminal>,
}
impl OwnedChild {
    fn spawn(
        root: &Path,
        backend: &str,
        namespace: &str,
        boundary: Boundary,
        phase: &str,
        terminals: &ChildTerminals,
        persist_upper: bool,
    ) -> Self {
        let module = module_path!().split_once("::").unwrap().1;
        let helper = format!("{module}::real_native_process_restart_helper");
        let log = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join(format!("{phase}.log")))
            .unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &helper,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("BREWFS_NATIVE_RESTART_ROOT", root)
            .env("BREWFS_NATIVE_RESTART_NAMESPACE", namespace)
            .env("BREWFS_NATIVE_RESTART_BACKEND", backend)
            .env("BREWFS_NATIVE_RESTART_BOUNDARY", boundary.spelling())
            .env("BREWFS_NATIVE_RESTART_PHASE", phase)
            .env(
                "BREWFS_NATIVE_RESTART_UPPER_PAYLOAD",
                if persist_upper {
                    "persistent"
                } else {
                    "metadata"
                },
            )
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        let receipt = Arc::new(ChildTerminal {
            pid: child.id(),
            terminal: AtomicBool::new(false),
        });
        let owned = Self {
            child,
            receipt: receipt.clone(),
        };
        terminals.lock().unwrap().push(receipt);
        owned
    }
    async fn wait(&mut self, deadline: Duration) -> ExitStatus {
        tokio::time::timeout(deadline, async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    self.receipt.terminal.store(true, Ordering::SeqCst);
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("owned process did not reach terminal status before deadline")
    }
    async fn kill_and_wait(&mut self) -> ExitStatus {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "staging child exited before parent death injection"
        );
        self.child.kill().unwrap();
        let status = self.wait(EXIT_WAIT).await;
        assert!(!status.success(), "death injection returned a success exit");
        status
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !self.receipt.terminal.load(Ordering::SeqCst) {
            // This guard owns only its exact Child handle/PID, never a process
            // name, service or the enclosing Cargo process group.
            let _ = self.child.kill();
            let deadline = Instant::now() + EXIT_WAIT;
            loop {
                match self.child.try_wait() {
                    Ok(Some(_)) => {
                        self.receipt.terminal.store(true, Ordering::SeqCst);
                        break;
                    }
                    Ok(None) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    _ => {
                        eprintln!(
                            "owned process {} has no terminal receipt after bounded kill/reap",
                            self.receipt.pid
                        );
                        break;
                    }
                }
            }
        }
    }
}

async fn run_children<B: WorkspaceKvBackend>(
    backend: Arc<B>,
    root: PathBuf,
    kind: &'static str,
    namespace: String,
    boundary: Boundary,
    terminals: ChildTerminals,
    persist_upper: bool,
) {
    let mut staging = OwnedChild::spawn(
        &root,
        kind,
        &namespace,
        boundary,
        "stage",
        &terminals,
        persist_upper,
    );
    let staged_pid = staging.child.id();
    tokio::time::timeout(CHILD_WAIT, async {
        loop {
            assert!(
                staging.child.try_wait().unwrap().is_none(),
                "staging child failed; see stage.log in the owned diagnostic directory"
            );
            if root.join("staged.json").is_file() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("staging durable-boundary deadline");
    let facts: RestartFacts = read_state(&root, "staged.json");
    assert_eq!(facts.staged_pid, staged_pid);
    assert_eq!(facts.boundary, boundary);
    assert_eq!(facts.upper_payload.is_some(), persist_upper);
    if let Some(upper) = &facts.upper_payload {
        assert_persistent_blocks(&root, upper);
    }
    let control = test_entity_state(backend.as_ref()).await;
    assert_eq!(
        control.journals[&facts.native_journal].phase,
        match boundary {
            Boundary::Prepare => SealPhase::Prepare,
            Boundary::Quiesced => SealPhase::Quiesced,
            Boundary::Pnb => SealPhase::Hashed,
        }
    );
    if let Some(ppj) = facts.ppj {
        assert_eq!(
            point(backend.as_ref(), &journal_key(ppj), 48 << 10).await,
            facts.ppj_bytes
        );
    }
    staging.kill_and_wait().await;
    assert!(
        staging.receipt.terminal.load(Ordering::SeqCst),
        "recovery started before original process was reaped"
    );
    let mut recovering = OwnedChild::spawn(
        &root,
        kind,
        &namespace,
        boundary,
        "recover",
        &terminals,
        persist_upper,
    );
    let recovered_pid = recovering.child.id();
    assert_ne!(staged_pid, recovered_pid);
    let status = recovering.wait(CHILD_WAIT).await;
    assert!(
        status.success(),
        "second process failed; see recover.log in the owned diagnostic directory"
    );
    let success: RestartSuccess = read_state(&root, "recovered.json");
    assert_eq!(success.schema, 1);
    assert_eq!(success.staged_pid, staged_pid);
    assert_eq!(success.recovered_pid, recovered_pid);
    assert_eq!(success.boundary, boundary);
    assert_eq!(success.old_lease, facts.old_lease);
    assert_ne!(success.current_lease, success.old_lease);
    assert_eq!(success.current_generation, facts.old_generation + 1);
    assert_eq!(
        success.upper_inode,
        facts.upper_payload.as_ref().map(|upper| upper.inode)
    );
    assert_eq!(
        point(
            backend.as_ref(),
            &packed_current_key(facts.workspace),
            48 << 10
        )
        .await,
        Some(success.target)
    );
    assert_eq!(
        PackedJournalRecord::decode(
            &point(
                backend.as_ref(),
                &journal_key(success.committed_ppj),
                48 << 10
            )
            .await
            .unwrap()
        )
        .unwrap()
        .phase,
        PackedJournalPhase::Committed
    );
}

async fn cleanup_namespace<B: WorkspaceKvBackend>(backend: &B) -> Result<(), WorkspaceError> {
    // A UUID namespace contains this test's keys only. Page deletion stays
    // bounded even when the child panics partway through catalog publication.
    for _ in 0..128 {
        let rows = backend
            .scan_prefix_page_with_byte_limits(
                b"",
                None,
                KvReadLimits {
                    max_records: 32,
                    max_key_bytes: 512,
                    max_value_bytes: 64 << 10,
                    max_total_bytes: 1 << 20,
                    max_response_bytes: 2 << 20,
                    max_data_requests: 32,
                },
            )
            .await?;
        if rows.is_empty() {
            backend.shutdown_metadata_backend().await?;
            return Ok(());
        }
        let checks = rows
            .iter()
            .map(|row| KvCheck {
                key: row.key.clone(),
                expected: Some(row.value.clone()),
            })
            .collect::<Vec<_>>();
        let writes = rows
            .iter()
            .map(|row| KvWrite::Delete {
                key: row.key.clone(),
            })
            .collect::<Vec<_>>();
        if !backend.compare_and_swap(&checks, &writes).await? {
            return Err(WorkspaceError::Busy);
        }
    }
    Err(WorkspaceError::Backend(
        "owned restart namespace exceeded cleanup row bound".into(),
    ))
}

async fn isolated<B: WorkspaceKvBackend>(
    backend: B,
    kind: &'static str,
    namespace: String,
    boundary: Boundary,
    persist_upper: bool,
) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().canonicalize().unwrap();
    let backend = Arc::new(backend);
    let worker = backend.clone();
    let terminals: ChildTerminals = Arc::new(std::sync::Mutex::new(Vec::new()));
    let worker_terminals = terminals.clone();
    let result = tokio::spawn(async move {
        run_children(
            worker,
            path,
            kind,
            namespace,
            boundary,
            worker_terminals,
            persist_upper,
        )
        .await
    })
    .await;
    let unresolved = terminals
        .lock()
        .unwrap()
        .iter()
        .filter(|receipt| !receipt.terminal.load(Ordering::SeqCst))
        .map(|receipt| receipt.pid)
        .collect::<Vec<_>>();
    if !unresolved.is_empty() {
        let path = root.keep();
        panic!(
            "exact child PIDs {unresolved:?} have no terminal receipt; KV cleanup withheld; owned diagnostics at {}",
            path.display()
        );
    }
    let cleanup = tokio::time::timeout(CLEANUP_WAIT, cleanup_namespace(backend.as_ref())).await;
    if result.is_err() || !cleanup.as_ref().is_ok_and(|result| result.is_ok()) {
        let path = root.keep();
        eprintln!(
            "owned process-restart diagnostics retained at {}",
            path.display()
        );
    }
    cleanup
        .expect("owned KV namespace cleanup deadline")
        .expect("owned KV namespace cleanup failure");
    if let Err(error) = result {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("process-restart parent task cancelled: {error}");
    }
}

async fn redis(boundary: Boundary) {
    redis_case(boundary, false).await;
}
async fn redis_case(boundary: Boundary, persist_upper: bool) {
    let namespace = format!("native-process-{}", Uuid::new_v4());
    let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
    isolated(
        RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap(),
        "redis",
        namespace,
        boundary,
        persist_upper,
    )
    .await;
}
async fn tikv(boundary: Boundary) {
    tikv_case(boundary, false).await;
}
async fn tikv_case(boundary: Boundary, persist_upper: bool) {
    let namespace = format!("native-process-{}", Uuid::new_v4());
    let Some(endpoints) = crate::test_support::tikv_endpoints() else {
        eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
        return;
    };
    isolated(
        TiKvWorkspaceBackend::connect(endpoints, &namespace)
            .await
            .unwrap(),
        "tikv",
        namespace,
        boundary,
        persist_upper,
    )
    .await;
}

async fn run_helper<B: WorkspaceKvBackend>(
    backend: Arc<B>,
    root: &Path,
    boundary: Boundary,
    phase: &str,
    persist_upper: bool,
) {
    if persist_upper {
        let upper = persistent_upper(root, phase).await;
        if phase == "stage" {
            stage(backend, root, boundary, upper, true).await;
        } else {
            recover(backend, root, boundary, upper, true).await;
        }
    } else {
        let upper = Arc::new(InMemoryBlockStore::new());
        if phase == "stage" {
            stage(backend, root, boundary, upper, false).await;
        } else {
            recover(backend, root, boundary, upper, false).await;
        }
    }
}

#[tokio::test]
#[ignore = "internal owned-process helper; invoke only from parent restart selectors"]
async fn real_native_process_restart_helper() {
    let root =
        PathBuf::from(std::env::var_os("BREWFS_NATIVE_RESTART_ROOT").expect("owned process root"))
            .canonicalize()
            .unwrap();
    let namespace = std::env::var("BREWFS_NATIVE_RESTART_NAMESPACE").expect("owned namespace");
    assert!(namespace.starts_with("native-process-"));
    Uuid::parse_str(namespace.strip_prefix("native-process-").unwrap()).unwrap();
    let boundary = Boundary::parse(&std::env::var("BREWFS_NATIVE_RESTART_BOUNDARY").unwrap());
    let phase = std::env::var("BREWFS_NATIVE_RESTART_PHASE").unwrap();
    assert!(matches!(phase.as_str(), "stage" | "recover"));
    let persist_upper = match std::env::var("BREWFS_NATIVE_RESTART_UPPER_PAYLOAD")
        .unwrap()
        .as_str()
    {
        "metadata" => false,
        "persistent" => true,
        _ => panic!("invalid owned upper payload mode"),
    };
    match std::env::var("BREWFS_NATIVE_RESTART_BACKEND")
        .unwrap()
        .as_str()
    {
        "redis" => {
            let url = std::env::var("BREWFS_TEST_REDIS_URL").expect("BREWFS_TEST_REDIS_URL");
            let backend = Arc::new(
                RedisWorkspaceBackend::connect(&url, &namespace)
                    .await
                    .unwrap(),
            );
            run_helper(backend, &root, boundary, &phase, persist_upper).await;
        }
        "tikv" => {
            let Some(endpoints) = crate::test_support::tikv_endpoints() else {
                eprintln!("skip: BREWFS_TEST_TIKV_PD_ENDPOINTS is unset or empty");
                return;
            };
            let backend = Arc::new(
                TiKvWorkspaceBackend::connect(endpoints, &namespace)
                    .await
                    .unwrap(),
            );
            run_helper(backend, &root, boundary, &phase, persist_upper).await;
        }
        _ => panic!("unsupported real process-restart backend"),
    }
}

#[tokio::test]
#[ignore = "actual killed process then independent Redis recovery; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_process_death_prepare_then_independent_recovery() {
    redis(Boundary::Prepare).await
}
#[tokio::test]
#[ignore = "actual killed process then independent TiKV recovery; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_process_death_prepare_then_independent_recovery() {
    tikv(Boundary::Prepare).await
}
#[tokio::test]
#[ignore = "actual killed process then independent Redis recovery; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_process_death_quiesced_then_independent_recovery() {
    redis(Boundary::Quiesced).await
}
#[tokio::test]
#[ignore = "actual killed process then independent TiKV recovery; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_process_death_quiesced_then_independent_recovery() {
    tikv(Boundary::Quiesced).await
}
#[tokio::test]
#[ignore = "actual killed process then independent Redis recovery; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_process_death_pnb_then_independent_recovery() {
    redis(Boundary::Pnb).await
}
#[tokio::test]
#[ignore = "actual killed process then independent TiKV recovery; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_process_death_pnb_then_independent_recovery() {
    tikv(Boundary::Pnb).await
}

#[tokio::test]
#[ignore = "actual persisted VFS upper payload and killed process then Redis Prepare recovery; requires BREWFS_TEST_REDIS_URL"]
async fn real_redis_process_death_prepare_with_persistent_upper_payload() {
    redis_case(Boundary::Prepare, true).await
}
#[tokio::test]
#[ignore = "actual persisted VFS upper payload and killed process then TiKV Prepare recovery; requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
async fn real_tikv_process_death_prepare_with_persistent_upper_payload() {
    tikv_case(Boundary::Prepare, true).await
}
