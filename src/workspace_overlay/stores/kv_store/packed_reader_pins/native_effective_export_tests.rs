//! Real original-ID native capture -> producer -> compare -> two native CAS.
//! No injected source proof, no raw digest builder and no topology publication.

#[path = "packed_merged_permission_tests.rs"]
mod packed_merged_permission_tests;

#[path = "packed_raw_bytes_tests.rs"]
mod packed_raw_bytes_tests;

#[path = "native_payload_budget_tests.rs"]
mod native_payload_budget_tests;

#[path = "packed_enumeration_tests.rs"]
mod packed_enumeration_tests;

#[path = "performance_tests.rs"]
mod performance_tests;

#[path = "cache_commit_regression_tests.rs"]
mod cache_commit_regression_tests;

use super::*;
use crate::cadapter::client::ObjectClient;
use crate::cadapter::localfs::LocalFsBackend;
use crate::chunk::ChunkLayout;
use crate::chunk::read_plan::{ReadSource, WorkspaceReadPlanProvider, execute_unified_into};
use crate::chunk::store::InMemoryBlockStore;
use crate::meta::layer::MetaLayer;
use crate::meta::store::AclRule;
use crate::vfs::config::VFSConfig;
use crate::vfs::fs::VFS;
use crate::workspace_overlay::catalog::VersionedMutation;
use crate::workspace_overlay::digest::{delta_digest, root_hash};
use crate::workspace_overlay::ids::JournalId;
use crate::workspace_overlay::meta_layer::{
    PinnedCatalogPackedBindingAuthority, WorkspaceMetaLayer,
};
use crate::workspace_overlay::model::{
    DentryDelta, DentryOp, InodeState, LayerState, SealPhase, ViewContext,
};
use crate::workspace_overlay::packed_reader_lifecycle::{
    PackedReaderLeaseOptions, PackedReaderSession,
};
use crate::workspace_overlay::packed_v3::PackedV3ReadonlyMeta;
use crate::workspace_overlay::packed_v3::wire005::{
    AuthenticatedV3Snapshot, FrozenNativeArtifact, NativeCaptureLimits, V3ProducerOptions,
};
use crate::workspace_overlay::packed_v3::{AccessProfile, PackedCodec, SizeClassTable};
use crate::workspace_overlay::stores::kv_store::packed_native_freeze::{
    FrozenNativeDeltaHash, NativeDeltaHashLimits,
};
use tokio_util::sync::CancellationToken;

type NativeMeta = WorkspaceMetaLayer<KvWorkspaceStore<PinMemoryBackend>>;
type NativeVfs = VFS<InMemoryBlockStore, NativeMeta>;

const CHUNK: u64 = 4096;
const OVERWRITE: &[u8] = b"upper-overwrite";
const TAIL: &[u8] = b"upper-tail";
const RAW_ALIAS: &[u8] = b"raw-\xff-alias";

struct Fixture {
    directory: tempfile::TempDir,
    client: ObjectClient<LocalFsBackend>,
    store: Arc<KvWorkspaceStore<PinMemoryBackend>>,
    backend: PinMemoryBackend,
    reader: Arc<dyn PackedReaderSession>,
    budget: Arc<V3MountBudget>,
    meta: Arc<NativeMeta>,
    vfs: NativeVfs,
    guard: HeadGuard,
    payload: Vec<u8>,
    binding: crate::workspace_overlay::publish::binding::PackedLowerBindingRecord,
}

async fn fixture(wrong_vfs_upper: bool) -> Fixture {
    fixture_from_packed(wrong_vfs_upper, packed().await).await
}

async fn fixture_from_packed(
    wrong_vfs_upper: bool,
    input: (
        tempfile::TempDir,
        ObjectClient<LocalFsBackend>,
        AuthenticatedV3Snapshot,
        crate::workspace_overlay::publish::binding::VerifiedPackedLower,
        Vec<u8>,
    ),
) -> Fixture {
    fixture_with_lease(wrong_vfs_upper, input, None).await
}

/// `lease`: bind the upper row cache (60 s TTL) to this proof, as the CLI does.
async fn fixture_with_lease(
    wrong_vfs_upper: bool,
    input: (
        tempfile::TempDir,
        ObjectClient<LocalFsBackend>,
        AuthenticatedV3Snapshot,
        crate::workspace_overlay::publish::binding::VerifiedPackedLower,
        Vec<u8>,
    ),
    lease: Option<Arc<crate::workspace_overlay::lifecycle::LeaseValidity>>,
) -> Fixture {
    let (directory, client, snapshot, lower_proof, payload) = input;
    let backend = PinMemoryBackend::default();
    backend.now.store(1_000_000_000, Ordering::SeqCst);
    let budget = V3MountBudget::defaults();
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
    let lower = Arc::new(
        PackedV3ReadonlyMeta::from_v3_budget(client.clone(), snapshot, CHUNK, 0, budget.clone())
            .unwrap(),
    );
    let upper = Arc::new(InMemoryBlockStore::new());
    let layout = ChunkLayout {
        chunk_size: CHUNK,
        block_size: u32::try_from(CHUNK).unwrap(),
    };
    let authority = Arc::new(PinnedCatalogPackedBindingAuthority {
        store: store.clone(),
        reader: reader.clone(),
    });
    let mut meta_layer = WorkspaceMetaLayer::with_chunk_size(
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
    .with_packed_v3_lower(
        binding.binding.clone(),
        lower,
        authority,
        upper.clone(),
        layout,
    )
    .unwrap();
    if let Some(lease) = lease {
        meta_layer = meta_layer
            .with_upper_row_cache(std::time::Duration::from_secs(60))
            .with_lease_bound_row_cache(lease);
    }
    let meta = Arc::new(meta_layer);
    meta.initialize().await.unwrap();
    let vfs_upper = if wrong_vfs_upper {
        Arc::new(InMemoryBlockStore::new())
    } else {
        upper
    };
    let provider: Arc<dyn WorkspaceReadPlanProvider> = meta.clone();
    let vfs = VFS::from_readonly_components_with_provider(
        VFSConfig::new(layout),
        vfs_upper,
        meta.clone(),
        provider,
    )
    .unwrap();
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
    }
}

fn limits() -> NativeCaptureLimits {
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

fn options() -> V3ProducerOptions {
    V3ProducerOptions {
        snapshot_id: [0x42; 32],
        root_dir_key: [0x73; 32],
        root_inode: 1,
        profile: AccessProfile::RandomSmallFile,
        size_classes: SizeClassTable::default(),
        build_policy: Default::default(),
        metadata_codec: PackedCodec::Raw,
        data_codec: PackedCodec::Raw,
    }
}

async fn add_raw_alias(f: &Fixture) {
    let expected_layers = f
        .store
        .load_layer_chain(f.guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let mut inode = f
        .store
        .load_layer_delta(f.guard.expected_head_layer_id)
        .await
        .unwrap()
        .inodes
        .into_iter()
        .find(|inode| inode.ino == 400)
        .expect("real lower copy-up inode");
    inode.nlink += 1;
    inode.sequence = 0;
    let native_kind = inode.kind;
    let mut mutation = VersionedMutation::empty(f.guard.clone(), expected_layers, CHUNK);
    mutation.inodes.push(inode);
    mutation.dentries.push(DentryDelta::put(
        f.guard.expected_head_layer_id,
        1,
        RAW_ALIAS.to_vec(),
        400,
        native_kind,
        0,
    ));
    f.store.apply_versioned_mutation(mutation).await.unwrap();
}

async fn candidate_bytes_and_holes(
    candidate: &PackedV3ReadonlyMeta<LocalFsBackend>,
) -> (Vec<u8>, Vec<(u64, u64)>) {
    let mut result = vec![0x55; 8192];
    let mut holes = Vec::new();
    for chunk in 0..2 {
        let prepared = candidate
            .prepare_unified_read(400, chunk, 0, CHUNK)
            .await
            .unwrap()
            .unwrap();
        for segment in &prepared.plan.segments {
            if matches!(&segment.source, ReadSource::Hole) {
                let begin = chunk * CHUNK + segment.logical_offset;
                holes.push((begin, begin + segment.length));
            }
        }
        execute_unified_into(
            prepared.fetcher.as_ref(),
            0,
            &prepared.plan,
            &mut result[(chunk * CHUNK) as usize..((chunk + 1) * CHUNK) as usize],
        )
        .await
        .unwrap();
    }
    (result, holes)
}

#[tokio::test]
async fn actual_native_effective_roundtrip_preserves_ids_hardlinks_holes_cold_and_native_hash() {
    let f = fixture(false).await;
    assert_eq!(f.vfs.stat("/nonzero").await.unwrap().ino, 400);
    let alias = f.vfs.link("/nonzero", "/alias").await.unwrap();
    assert_eq!((alias.ino, alias.nlink), (400, 2));
    let fh = f
        .vfs
        .open(
            400,
            f.vfs.stat("/nonzero").await.unwrap(),
            true,
            true,
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        f.vfs.write(fh, 128, OVERWRITE).await.unwrap(),
        OVERWRITE.len()
    );
    f.vfs.flush(fh).await.unwrap();
    f.vfs
        .workspace_hole_fallocate_from_fuse(fh, 400, 512, 257, true)
        .await
        .unwrap();
    f.vfs.truncate_inode(400, 8192).await.unwrap();
    assert_eq!(f.vfs.write(fh, 4352, TAIL).await.unwrap(), TAIL.len());
    f.vfs.flush(fh).await.unwrap();
    f.vfs.close(fh).await.unwrap();
    let (symlink, _) = f.vfs.create_symlink("/symlink", "/nonzero").await.unwrap();
    assert!(symlink > 400);
    let xattr: Vec<u8> = (0..65536).map(|index| (index % 251) as u8).collect();
    f.vfs
        .set_xattr_bytes_ino(400, b"user.boundary", &xattr, 0)
        .await
        .unwrap();
    let acl = AclRule {
        acl_type: 1,
        qualifier: 42,
        permissions: 5,
    };
    f.vfs.set_acl_ino(400, acl.clone()).await.unwrap();
    let deleted = f.vfs.create_file("/deleted-native").await.unwrap();
    f.vfs.unlink("/deleted-native").await.unwrap();
    add_raw_alias(&f).await;

    let mut expected = vec![0; 8192];
    expected[..CHUNK as usize].copy_from_slice(&f.payload);
    expected[128..128 + OVERWRITE.len()].copy_from_slice(OVERWRITE);
    expected[512..769].fill(0);
    expected[4352..4352 + TAIL.len()].copy_from_slice(TAIL);
    // Freeze real local admission/metadata/upload work before native Q.
    let local = f.vfs.quiesce_packed_vfs().await.unwrap();
    let expected_layers = f
        .store
        .load_layer_chain(f.guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let native_delta = f
        .store
        .load_layer_delta(f.guard.expected_head_layer_id)
        .await
        .unwrap();
    assert!(
        native_delta
            .inodes
            .iter()
            .any(|row| row.ino == deleted && row.state == InodeState::Deleted)
    );
    assert!(
        native_delta
            .dentries
            .iter()
            .any(|row| row.name == b"deleted-native" && row.op == DentryOp::Whiteout)
    );
    assert!(
        native_delta.acls.iter().any(
            |row| row.ino == 400 && row.value.as_deref() == Some(5u32.to_be_bytes().as_slice())
        )
    );
    let expected_delta = delta_digest(&native_delta).unwrap();
    let parent = f
        .store
        .load_layer(f.guard.expected_head_layer_id)
        .await
        .unwrap()
        .parent_layer_id
        .unwrap();
    let expected_root = root_hash(
        f.store.load_layer(parent).await.unwrap().root_hash.unwrap(),
        expected_delta,
    );
    let journal_id = JournalId::new();
    let planned_head = LayerId::new();
    let native = Arc::new(
        f.store
            .clone()
            .begin_packed_native_quiesce(
                f.guard.clone(),
                expected_layers,
                journal_id,
                planned_head,
                f.budget.clone(),
            )
            .await
            .unwrap(),
    );
    let immutable_receipt = native.canonical_receipt_bytes().to_vec();
    let source = FrozenNativeArtifact::capture(
        native.clone(),
        local,
        f.directory.path().to_path_buf(),
        limits(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let (source, manifest) = source
        .produce_candidate(
            f.client.clone(),
            f.directory.path().to_path_buf(),
            "native-effective-test/staged".into(),
            uuid::Uuid::new_v4(),
            options(),
        )
        .await
        .unwrap();
    assert!(
        manifest.key.contains("/native-incarnation-"),
        "producer requires fresh physical incarnation"
    );
    let snapshot = AuthenticatedV3Snapshot::open(&f.client, &manifest)
        .await
        .unwrap();
    let candidate = Arc::new(
        PackedV3ReadonlyMeta::from_v3_budget(
            f.client.clone(),
            snapshot,
            CHUNK,
            0,
            f.budget.clone(),
        )
        .unwrap(),
    );
    assert_eq!(candidate.lookup(1, "nonzero").await.unwrap(), Some(400));
    assert_eq!(candidate.lookup(1, "alias").await.unwrap(), Some(400));
    assert_eq!(
        candidate
            .lookup_with_attr_bytes(1, RAW_ALIAS)
            .await
            .unwrap()
            .unwrap()
            .0,
        400
    );
    assert_eq!(candidate.stat_fresh(400).await.unwrap().unwrap().nlink, 3);
    assert_eq!(candidate.lookup(1, "symlink").await.unwrap(), Some(symlink));
    assert_eq!(
        candidate.read_symlink_bytes(symlink).await.unwrap(),
        b"/nonzero"
    );
    assert_eq!(
        candidate
            .get_xattr_bytes(400, b"user.boundary")
            .await
            .unwrap(),
        Some(xattr)
    );
    assert_eq!(
        candidate
            .get_acl(400, acl.acl_type, acl.qualifier)
            .await
            .unwrap(),
        Some(acl)
    );
    assert!(candidate.stat_fresh(deleted).await.unwrap().is_none());
    let (actual, holes) = candidate_bytes_and_holes(candidate.as_ref()).await;
    assert_eq!(actual, expected);
    for (begin, end) in [(512, 769), (4096, 4352), (4352 + TAIL.len() as u64, 8192)] {
        assert!(
            holes
                .iter()
                .any(|(hole_begin, hole_end)| *hole_begin <= begin && *hole_end >= end),
            "required native Hole [{begin},{end}) absent: {holes:?}"
        );
    }
    let verified = source.compare_candidate(candidate.clone()).await.unwrap();
    let source_digest = verified.source_digest();
    assert_eq!(
        (
            verified.counts().inodes,
            verified.counts().names,
            verified.counts().logical_bytes
        ),
        (3, 4, 8192)
    );
    assert_eq!(
        verified.counts().data_bytes,
        CHUNK - 257 + TAIL.len() as u64
    );
    assert_eq!(verified.highest_inode(), symlink);
    let native_hash = Arc::new(
        FrozenNativeDeltaHash::capture(
            native.clone(),
            f.reader.clone(),
            NativeDeltaHashLimits {
                max_native_delta_rows: 1000,
                max_canonical_bytes: 1 << 20,
            },
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        (native_hash.delta_digest(), native_hash.root_hash()),
        (expected_delta, expected_root)
    );

    // Hold the real DD CAS response after commit and inspect the actual native
    // catalog between the two production transitions, rather than phase flags.
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *f.backend.reply_barrier.lock().await = Some((entered.clone(), resume.clone()));
    let entered_wait = entered.notified();
    let hash_for_driver = native_hash.clone();
    let driver = tokio::spawn(async move { verified.promote_native_hashed(hash_for_driver).await });
    tokio::time::timeout(Duration::from_secs(3), entered_wait)
        .await
        .unwrap();
    let drained = f.store.load_seal_journal(journal_id).await.unwrap();
    assert_eq!(
        (
            drained.phase,
            drained.pending_bytes,
            drained.delta_digest,
            drained.root_hash
        ),
        (SealPhase::DataDrained, 0, None, None)
    );
    assert_eq!(
        f.store
            .load_workspace(f.guard.workspace_id)
            .await
            .unwrap()
            .head_layer_id,
        f.guard.expected_head_layer_id
    );
    resume.notify_one();
    let promoted = tokio::time::timeout(Duration::from_secs(10), driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let hashed = match promoted {
        Ok(hashed) => hashed,
        Err(failure) => panic!("actual source promotion failed: {}", failure.error),
    };
    hashed.validate().await.unwrap();
    assert_eq!(hashed.source_digest(), source_digest);
    assert_eq!(
        hashed.phase_authority().canonical_receipt_bytes(),
        immutable_receipt
    );
    assert_eq!(native.canonical_receipt_bytes(), immutable_receipt);
    let journal = f.store.load_seal_journal(journal_id).await.unwrap();
    assert_eq!(
        (journal.phase, journal.delta_digest, journal.root_hash),
        (SealPhase::Hashed, Some(expected_delta), Some(expected_root))
    );
    assert_eq!(hashed.phase_authority().journal(), &journal);
    // Native hashing did not perform the separately required carrier/PWB CAS.
    assert_eq!(
        f.store
            .load_workspace(f.guard.workspace_id)
            .await
            .unwrap()
            .head_layer_id,
        f.guard.expected_head_layer_id
    );
    assert_eq!(
        f.store
            .load_layer(f.guard.expected_head_layer_id)
            .await
            .unwrap()
            .state,
        LayerState::Sealing
    );
    assert!(f.store.load_layer(planned_head).await.is_err());
    drop(hashed);
    drop(native_hash);
    drop(native);
    drop(candidate);
    f.meta.shutdown_session().await.unwrap();
}

#[tokio::test]
async fn actual_native_capture_rejects_other_vfs_upper_without_catalog_write() {
    let f = fixture(true).await;
    let local = f.vfs.quiesce_packed_vfs().await.unwrap();
    let expected_layers = f
        .store
        .load_layer_chain(f.guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let journal_id = JournalId::new();
    let native = Arc::new(
        f.store
            .clone()
            .begin_packed_native_quiesce(
                f.guard.clone(),
                expected_layers,
                journal_id,
                LayerId::new(),
                f.budget.clone(),
            )
            .await
            .unwrap(),
    );
    let before = f.backend.rows.lock().await.clone();
    let result = FrozenNativeArtifact::capture(
        native.clone(),
        local,
        f.directory.path().to_path_buf(),
        limits(),
        CancellationToken::new(),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("other upper Arc accepted as frozen source"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("identity mismatch"), "{error}");
    assert_eq!(*f.backend.rows.lock().await, before);
    assert_eq!(
        f.store.load_seal_journal(journal_id).await.unwrap().phase,
        SealPhase::Quiesced
    );
    native.validate().await.unwrap();
    drop(native);
    f.meta.shutdown_session().await.unwrap();
}

#[tokio::test]
async fn lease_proven_read_fence_is_local_and_falls_back_to_remote_on_doubt() {
    use crate::chunk::read_plan::{ReadViewChanged, WorkspaceReadPlanProvider};
    use crate::workspace_overlay::lifecycle::LeaseValidity;
    let validity = Arc::new(LeaseValidity::new(
        std::time::Instant::now(),
        std::time::Duration::from_secs(60),
    ));
    let f = fixture_with_lease(false, packed().await, Some(validity.clone())).await;
    // Copy inode 400 up (a real chmod), then warm its cached rows.
    f.vfs.chmod(400, 0o644).await.unwrap();
    let warm = f
        .meta
        .begin_unified_read_request(400)
        .await
        .unwrap()
        .unwrap();
    warm.ensure_current().await.unwrap();
    drop(warm);
    // Lease proven and rows cached: begin + ensure_current issue no fence
    // round trip at all.
    let before = f.backend.timed_reads.load(Ordering::SeqCst);
    let request = f
        .meta
        .begin_unified_read_request(400)
        .await
        .unwrap()
        .unwrap();
    request.ensure_current().await.unwrap();
    request.ensure_current().await.unwrap();
    assert_eq!(f.backend.timed_reads.load(Ordering::SeqCst), before);
    // A local commit to the inode invalidates its cached rows: the open
    // request's fence falls back to the remote check, which sees the changed
    // rows and asks the reader to retry.
    f.vfs.chmod(400, 0o600).await.unwrap();
    let error = request.ensure_current().await.unwrap_err();
    assert!(error.downcast_ref::<ReadViewChanged>().is_some());
    assert!(f.backend.timed_reads.load(Ordering::SeqCst) > before);
    // Lease no longer proven: fences are remote again, and losing the real
    // lease fails the request closed.
    validity.revoke();
    let request = f
        .meta
        .begin_unified_read_request(400)
        .await
        .unwrap()
        .unwrap();
    let remote_before = f.backend.timed_reads.load(Ordering::SeqCst);
    request.ensure_current().await.unwrap();
    assert!(f.backend.timed_reads.load(Ordering::SeqCst) > remote_before);
    f.store
        .release_lease(crate::workspace_overlay::catalog::ReleaseLease {
            lease_id: f.guard.lease_id,
            holder_generation: f.guard.holder_generation,
        })
        .await
        .unwrap();
    let error = request.ensure_current().await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<WorkspaceError>(),
        Some(WorkspaceError::Fenced)
    ));
    f.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn lease_proven_actor_checks_reuse_cached_parent_permissions_but_see_chmod() {
    use crate::meta::layer::{NamespaceActor, scope_namespace_actor};
    use crate::workspace_overlay::lifecycle::LeaseValidity;
    let validity = Arc::new(LeaseValidity::new(
        std::time::Instant::now(),
        std::time::Duration::from_secs(60),
    ));
    let f = fixture_with_lease(false, packed().await, Some(validity.clone())).await;
    // A world-writable upper directory, so a non-root actor may create in it.
    let dir = f.vfs.mkdir_p("/actor-dir").await.unwrap();
    f.vfs.chmod(dir, 0o777).await.unwrap();
    let actor = NamespaceActor {
        uid: 4242,
        gid: 4242,
        groups: vec![4242],
    };
    let create = |name: &'static str| {
        let vfs = f.vfs.clone();
        let actor = actor.clone();
        async move {
            scope_namespace_actor(
                Some(actor),
                vfs.create_file_at_with_umask(dir, name, true, 0o644, 0, 4242, 4242),
            )
            .await
        }
    };
    create("warm").await.unwrap();
    // Parent (and root ancestor) permission rows are now cached.
    let first = f.backend.consistent_reads.load(Ordering::SeqCst);
    create("a").await.unwrap();
    let cached = f.backend.consistent_reads.load(Ordering::SeqCst) - first;
    // While the cache is authoritative, a local chmod removing write access
    // is still enforced on the very next create: the chmod commit
    // invalidated the cached parent rows.
    f.vfs.chmod(dir, 0o755).await.unwrap();
    assert!(create("denied").await.is_err());
    f.vfs.chmod(dir, 0o777).await.unwrap();
    create("warm-again").await.unwrap();
    // Without the lease proof every actor check reads the store again.
    validity.revoke();
    let start = f.backend.consistent_reads.load(Ordering::SeqCst);
    create("c").await.unwrap();
    let uncached = f.backend.consistent_reads.load(Ordering::SeqCst) - start;
    assert!(
        cached < uncached,
        "cached actor checks ({cached}) must read less than uncached ({uncached})"
    );
    f.reader.shutdown().await.unwrap();
}

/// Kept invariant: a live lease excludes a second holder (no split-brain).
#[tokio::test]
async fn invariant_second_mount_cannot_acquire_a_live_lease() {
    let f = fixture(false).await;
    let second = f
        .store
        .acquire_lease(crate::workspace_overlay::catalog::AcquireLease {
            workspace_id: f.guard.workspace_id,
            lease_id: crate::workspace_overlay::ids::LeaseId::new(),
            holder_generation: 99,
            ttl_ns: 30_000_000_000,
        })
        .await;
    assert!(matches!(second, Err(WorkspaceError::Busy)), "{second:?}");
    // The original holder still commits.
    f.vfs.create_file("/still-mine").await.unwrap();
    f.reader.shutdown().await.unwrap();
}

/// Kept invariant: once the lease has expired (backend clock), no commit by
/// the old holder succeeds, even though nothing else changed.
#[tokio::test]
async fn invariant_commit_after_lease_expiry_fails_closed() {
    let f = fixture(false).await;
    f.vfs.create_file("/before-expiry").await.unwrap();
    // Past the 30 s fixture lease (backend clock), with no renewal.
    f.backend.now.fetch_add(31_000_000_000, Ordering::SeqCst);
    assert!(f.vfs.create_file("/after-expiry").await.is_err());
    assert!(f.vfs.stat("/after-expiry").await.is_err());
    f.reader.shutdown().await.unwrap();
}

/// Kept invariant: losing the lease fences this mount, and while the packed
/// open record is held no other holder can take the workspace over.
#[tokio::test]
async fn invariant_commit_after_lease_loss_fails_closed_and_blocks_takeover() {
    let f = fixture(false).await;
    f.store
        .release_lease(crate::workspace_overlay::catalog::ReleaseLease {
            lease_id: f.guard.lease_id,
            holder_generation: f.guard.holder_generation,
        })
        .await
        .unwrap();
    assert!(f.vfs.create_file("/after-release").await.is_err());
    let takeover = f
        .store
        .acquire_lease(crate::workspace_overlay::catalog::AcquireLease {
            workspace_id: f.guard.workspace_id,
            lease_id: crate::workspace_overlay::ids::LeaseId::new(),
            holder_generation: 99,
            ttl_ns: 30_000_000_000,
        })
        .await;
    assert!(
        matches!(takeover, Err(WorkspaceError::Busy)),
        "{takeover:?}"
    );
    f.reader.shutdown().await.unwrap();
}

/// Kept invariant: no admin path rotates the head of a workspace whose
/// writable lease is live. Head-changing publication must present the live
/// lease itself; a foreign holder (or a fresh lease) is fenced and nothing
/// is written, so the mount keeps committing against an unchanged head.
#[tokio::test]
async fn invariant_no_head_change_under_an_active_lease() {
    let f = fixture(false).await;
    let expected_binding = f.binding.clone();
    let before_workspace = f.store.load_workspace(f.guard.workspace_id).await.unwrap();
    let layers: [crate::workspace_overlay::model::LayerRecord; 2] = f
        .store
        .load_layer_chain(f.guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    // A genuinely new manifest, so only the lease authority can refuse it.
    let (_new_objects, _, _, lower, _) =
        crate::workspace_overlay::stores::binding_tests::packed_with_snapshot_id([11; 32]).await;
    for foreign in [
        // Another holder generation of the same lease id.
        HeadGuard {
            holder_generation: f.guard.holder_generation + 1,
            ..f.guard.clone()
        },
        // A different lease id (an admin's own fresh lease).
        HeadGuard {
            lease_id: crate::workspace_overlay::ids::LeaseId::new(),
            ..f.guard.clone()
        },
    ] {
        let result = f
            .store
            .publish_packed_lower_binding(
                crate::workspace_overlay::publish::binding::PublishPackedLowerBinding {
                    guard: foreign,
                    expected_layers: layers.clone(),
                    expected_base: expected_binding.base_revision.clone(),
                    expected_binding: expected_binding.clone(),
                    lower: lower.clone(),
                },
            )
            .await;
        assert!(matches!(result, Err(WorkspaceError::Fenced)), "{result:?}");
    }
    // A competing lease cannot even be acquired while this one is live.
    let competing = f
        .store
        .acquire_lease(crate::workspace_overlay::catalog::AcquireLease {
            workspace_id: f.guard.workspace_id,
            lease_id: crate::workspace_overlay::ids::LeaseId::new(),
            holder_generation: 99,
            ttl_ns: 30_000_000_000,
        })
        .await;
    assert!(
        matches!(competing, Err(WorkspaceError::Busy)),
        "{competing:?}"
    );
    let after_workspace = f.store.load_workspace(f.guard.workspace_id).await.unwrap();
    assert_eq!(
        after_workspace.head_layer_id,
        before_workspace.head_layer_id
    );
    assert_eq!(after_workspace.head_epoch, before_workspace.head_epoch);
    assert_eq!(after_workspace.active_lease, Some(f.guard.lease_id));
    f.vfs.create_file("/head-unchanged").await.unwrap();
    f.reader.shutdown().await.unwrap();
}

/// Sole writer: a warm commit reuses its own last commit's post-image as the
/// CAS conditions and issues no view pre-read; a lease renewal (the only
/// expected outside change) costs one fallback re-read, never an error.
#[tokio::test]
async fn sole_writer_commit_skips_the_view_pre_read_and_survives_renewal() {
    let f = fixture(false).await;
    f.vfs.create_file("/warm-0").await.unwrap();
    let before = f.backend.view_reads.load(Ordering::SeqCst);
    for index in 1..=4 {
        f.vfs.create_file(&format!("/warm-{index}")).await.unwrap();
    }
    let warm = f.backend.view_reads.load(Ordering::SeqCst) - before;
    assert_eq!(warm, 0, "warm commits must not pre-read their view");
    // Renew the lease out of band one second later (backend clock), so the
    // cached lease bytes really go stale.
    f.backend.now.fetch_add(1_000_000_000, Ordering::SeqCst);
    f.store
        .renew_lease(crate::workspace_overlay::catalog::RenewLease {
            lease_id: f.guard.lease_id,
            holder_generation: f.guard.holder_generation,
            ttl_ns: 30_000_000_000,
        })
        .await
        .unwrap();
    let before = f.backend.view_reads.load(Ordering::SeqCst);
    f.vfs.create_file("/after-renew").await.unwrap();
    assert!(f.vfs.stat("/after-renew").await.is_ok());
    // The stale lease bytes fail one cached CAS; one fresh read recovers.
    assert_eq!(f.backend.view_reads.load(Ordering::SeqCst) - before, 1);
    for index in 0..=4 {
        assert!(f.vfs.stat(&format!("/warm-{index}")).await.is_ok());
    }
    // Losing the lease still fails closed even with a warm cache.
    f.store
        .release_lease(crate::workspace_overlay::catalog::ReleaseLease {
            lease_id: f.guard.lease_id,
            holder_generation: f.guard.holder_generation,
        })
        .await
        .unwrap();
    assert!(f.vfs.create_file("/after-release").await.is_err());
    f.reader.shutdown().await.unwrap();
}

/// A binding publication by the holder's own lease rewrites the workspace
/// and head records, which every cached commit still checks: the next commit
/// against the old binding cannot succeed from the cache and is fenced after
/// its fresh re-read.
#[tokio::test]
async fn cached_commit_cannot_outlive_a_binding_publication() {
    let f = fixture(false).await;
    f.vfs.create_file("/warm").await.unwrap();
    let layers: [crate::workspace_overlay::model::LayerRecord; 2] = f
        .store
        .load_layer_chain(f.guard.expected_head_layer_id)
        .await
        .unwrap()
        .try_into()
        .unwrap();
    let (_objects, _, _, lower, _) =
        crate::workspace_overlay::stores::binding_tests::packed_with_snapshot_id([12; 32]).await;
    f.store
        .publish_packed_lower_binding(
            crate::workspace_overlay::publish::binding::PublishPackedLowerBinding {
                guard: f.guard.clone(),
                expected_layers: layers,
                expected_base: f.binding.base_revision.clone(),
                expected_binding: f.binding.clone(),
                lower,
            },
        )
        .await
        .unwrap();
    let before = f.backend.view_reads.load(Ordering::SeqCst);
    assert!(f.vfs.create_file("/after-publish").await.is_err());
    assert!(
        f.backend.view_reads.load(Ordering::SeqCst) > before,
        "the stale cached view must fall back to a fresh read"
    );
    f.reader.shutdown().await.unwrap();
}

/// Concurrency oracle for any change to commit conflict detection: many
/// tasks concurrently create files in one directory, write their own files
/// and extend one shared file. No create may be lost or duplicated and no
/// size update may be lost, whatever the commit serialization strategy.
#[tokio::test]
async fn concurrency_oracle_no_lost_creates_or_size_updates() {
    use crate::chunk::SliceDesc;
    use crate::meta::layer::MetaLayer;
    const TASKS: u64 = 8;
    const ROUNDS: u64 = 6;
    let f = fixture(false).await;
    let shared = f.meta.create_file(1, "oracle-shared".into()).await.unwrap();
    let mut tasks = Vec::new();
    for task in 0..TASKS {
        let meta = f.meta.clone();
        tasks.push(tokio::spawn(async move {
            for round in 0..ROUNDS {
                let ino = meta
                    .create_file(1, format!("oracle-{task}-{round}"))
                    .await
                    .unwrap();
                for (target, offset, slice) in [
                    (ino, 0, 20_000 + task * 100 + round * 2),
                    (
                        shared,
                        (task * ROUNDS + round) * 64,
                        20_001 + task * 100 + round * 2,
                    ),
                ] {
                    let chunk_id = crate::vfs::chunk_id_for(target, 0).unwrap();
                    meta.write(
                        target,
                        chunk_id,
                        SliceDesc {
                            slice_id: slice,
                            chunk_id,
                            offset,
                            length: 64,
                        },
                        offset + 64,
                    )
                    .await
                    .unwrap();
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    for task in 0..TASKS {
        for round in 0..ROUNDS {
            let ino = f
                .meta
                .lookup(1, &format!("oracle-{task}-{round}"))
                .await
                .unwrap()
                .expect("a committed create was lost");
            assert_eq!(f.meta.stat(ino).await.unwrap().unwrap().size, 64);
        }
    }
    assert_eq!(
        f.meta.stat(shared).await.unwrap().unwrap().size,
        TASKS * ROUNDS * 64,
        "a concurrent size update to the shared file was lost"
    );
    f.reader.shutdown().await.unwrap();
}

/// Group commit: many concurrent writes share commits. Every write lands
/// with its own extent and size; a write whose inode is gone fails alone
/// without failing the writes it was batched with; a write returns only
/// after its batch committed (so fsync after write sees it durable).
#[tokio::test]
async fn group_committed_writes_land_individually_and_fail_individually() {
    use crate::chunk::SliceDesc;
    use crate::meta::layer::MetaLayer;
    let f = fixture(false).await;
    let mut inodes = Vec::new();
    for index in 0..24 {
        inodes.push(f.meta.create_file(1, format!("gc-{index}")).await.unwrap());
    }
    let gone = 9_999_999;
    let mut tasks = Vec::new();
    for (index, ino) in inodes
        .iter()
        .copied()
        .chain(std::iter::once(gone))
        .enumerate()
    {
        let meta = f.meta.clone();
        tasks.push(tokio::spawn(async move {
            let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();
            let result = meta
                .write(
                    ino,
                    chunk_id,
                    SliceDesc {
                        slice_id: 30_000 + index as u64,
                        chunk_id,
                        offset: 0,
                        length: 100 + index as u64,
                    },
                    100 + index as u64,
                )
                .await;
            (ino, index as u64, result)
        }));
    }
    for task in tasks {
        let (ino, index, result) = task.await.unwrap();
        if ino == gone {
            assert!(
                matches!(result, Err(crate::meta::store::MetaError::NotFound(_))),
                "{result:?}"
            );
            continue;
        }
        result.unwrap();
        // Returned => committed: visible right away with its own size.
        assert_eq!(f.meta.stat(ino).await.unwrap().unwrap().size, 100 + index);
    }
    f.reader.shutdown().await.unwrap();
}

/// Writes to one file batched together keep every extent and the maximum
/// size; the later of two overlapping writes wins on the overlap.
#[tokio::test]
async fn group_commit_same_file_keeps_order_and_size() {
    use crate::chunk::SliceDesc;
    use crate::meta::layer::MetaLayer;
    let f = fixture(false).await;
    let ino = f.meta.create_file(1, "gc-same".into()).await.unwrap();
    let chunk_id = crate::vfs::chunk_id_for(ino, 0).unwrap();
    let mut tasks = Vec::new();
    for index in 0..16u64 {
        let meta = f.meta.clone();
        tasks.push(tokio::spawn(async move {
            meta.write(
                ino,
                chunk_id,
                SliceDesc {
                    slice_id: 40_000 + index,
                    chunk_id,
                    offset: index * 64,
                    length: 64,
                },
                index * 64 + 64,
            )
            .await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(f.meta.stat(ino).await.unwrap().unwrap().size, 16 * 64);
    // Sequential overlapping writes: the second must win where they overlap.
    for (slice_id, length) in [(41_000u64, 128u64), (41_001, 64)] {
        f.meta
            .write(
                ino,
                chunk_id,
                SliceDesc {
                    slice_id,
                    chunk_id,
                    offset: 0,
                    length,
                },
                16 * 64,
            )
            .await
            .unwrap();
    }
    use crate::workspace_overlay::model::ExtentKind;
    let mut rows = f
        .store
        .get_extent_deltas(crate::workspace_overlay::catalog::ExtentQuery {
            layer_ids: vec![f.guard.expected_head_layer_id],
            ino,
            chunk_index: 0,
            range_start: 0,
            range_end: 128,
        })
        .await
        .unwrap();
    rows.sort_by_key(|row| row.sequence);
    let slices = rows
        .iter()
        .filter_map(|row| match row.kind {
            ExtentKind::Data { slice_id, .. } if slice_id >= 41_000 => Some(slice_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    // Arrival order is preserved in sequence order, so 41_001 resolves over
    // 41_000 on their overlap (newest-wins).
    assert_eq!(slices, vec![41_000, 41_001], "{rows:?}");
    f.reader.shutdown().await.unwrap();
}

/// A create seeds the fresh inode's paths from its parent's cached paths;
/// they must equal what an uncached reverse walk computes, at the root and
/// in a subdirectory.
#[tokio::test]
async fn created_paths_are_seeded_exactly() {
    use crate::meta::layer::MetaLayer;
    let validity = Arc::new(crate::workspace_overlay::lifecycle::LeaseValidity::new(
        std::time::Instant::now(),
        std::time::Duration::from_secs(30),
    ));
    let seeded = fixture_with_lease(false, packed().await, Some(validity)).await;
    let dir = seeded.meta.mkdir(1, "seed-dir".into()).await.unwrap();
    // Cache the directory's complete paths, then create under it and root.
    let dir_paths = seeded.meta.get_paths_bytes(dir).await.unwrap();
    let nested = seeded.meta.create_file(dir, "nested".into()).await.unwrap();
    let top = seeded.meta.create_file(1, "top".into()).await.unwrap();
    let nested_seeded = seeded.meta.get_paths_bytes(nested).await.unwrap();
    let top_seeded = seeded.meta.get_paths_bytes(top).await.unwrap();
    assert_eq!(dir_paths, vec![b"/seed-dir".to_vec()]);
    assert_eq!(nested_seeded, vec![b"/seed-dir/nested".to_vec()]);
    assert_eq!(top_seeded, vec![b"/top".to_vec()]);
    // Drop every cache: a full reverse walk over the store must agree.
    seeded.meta.invalidate_upper_rows();
    assert_eq!(
        seeded.meta.get_paths_bytes(nested).await.unwrap(),
        nested_seeded
    );
    assert_eq!(seeded.meta.get_paths_bytes(top).await.unwrap(), top_seeded);
    seeded.reader.shutdown().await.unwrap();
}

#[tokio::test]
async fn packed_workspace_filesystem_57_byte_write_and_readback_complete_within_deadline() {
    use asyncfuse::raw::{Filesystem, Request};
    use std::ffi::OsStr;
    use std::time::Duration;
    use tokio::time::timeout;

    const WAIT: Duration = Duration::from_secs(5);
    let f = timeout(WAIT, fixture(false))
        .await
        .expect("packed fixture setup exceeded five seconds");
    let request = |unique| Request {
        unique,
        pid: std::process::id(),
        ..Request::default()
    };
    let created = timeout(
        WAIT,
        Filesystem::create(
            &f.vfs,
            request(1),
            1,
            OsStr::new("write-diagnostic"),
            0o644,
            libc::O_WRONLY as u32,
        ),
    )
    .await
    .expect("actual Filesystem::create exceeded five seconds")
    .unwrap();
    let ino = created.attr.ino;
    assert_ne!(created.fh, 0);
    let payload = [0x5a; 57];
    let written = timeout(
        WAIT,
        Filesystem::write(
            &f.vfs,
            request(2),
            ino,
            created.fh,
            0,
            &payload,
            0,
            libc::O_WRONLY as u32,
        ),
    )
    .await
    .expect("actual Filesystem::write exceeded five seconds")
    .unwrap();
    assert_eq!(written.written, 57);
    timeout(
        WAIT,
        Filesystem::fsync(&f.vfs, request(3), ino, created.fh, false),
    )
    .await
    .expect("actual Filesystem::fsync exceeded five seconds")
    .unwrap();
    timeout(
        WAIT,
        Filesystem::release(
            &f.vfs,
            request(4),
            ino,
            created.fh,
            libc::O_WRONLY as u32,
            0,
            false,
        ),
    )
    .await
    .expect("actual writer release exceeded five seconds")
    .unwrap();
    let opened = timeout(
        WAIT,
        Filesystem::open(&f.vfs, request(5), ino, libc::O_RDONLY as u32),
    )
    .await
    .expect("actual read open exceeded five seconds")
    .unwrap();
    let read = timeout(
        WAIT,
        Filesystem::read(&f.vfs, request(6), ino, opened.fh, 0, 57),
    )
    .await
    .expect("actual Filesystem::read exceeded five seconds")
    .unwrap();
    assert_eq!(read.data.as_ref(), payload.as_slice());
    drop(read);
    timeout(
        WAIT,
        Filesystem::release(
            &f.vfs,
            request(7),
            ino,
            opened.fh,
            libc::O_RDONLY as u32,
            0,
            false,
        ),
    )
    .await
    .expect("actual reader release exceeded five seconds")
    .unwrap();
    assert_eq!(f.vfs.open_file_handle_count(), 0);
    timeout(WAIT, f.reader.shutdown())
        .await
        .expect("packed reader shutdown exceeded five seconds")
        .unwrap();
}
