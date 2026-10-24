//! Actual KV permission snapshots raced with the concrete joint mount renewal.
//! The backend wrapper controls scheduling and transport results only; all
//! catalog, writer authority, permission merging and renewal code is real.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, Semaphore};

use crate::chunk::{BlockKey, BlockStore, ChunkLayout};
use crate::meta::MetaLayer;
use crate::workspace_overlay::catalog::{
    HeadGuard, PermissionSnapshotQuery, ReleaseLease, VersionedMutation, WorkspaceStore,
};
use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::lifecycle::{
    PackedMountAuthority, PackedMountCatalog, PackedMountSessionRequest,
};
use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
use crate::workspace_overlay::model::{LayerRecord, SnapshotLease};
use crate::workspace_overlay::packed_v3::PackedV3ReadonlyMeta;
use crate::workspace_overlay::stores::binding_tests::{
    packed, packed_open_preparation_test_backend, request,
};
use crate::workspace_overlay::stores::kv_backend::{
    KvCheck, KvEntry, KvReadLimits, KvWrite, WorkspaceKvBackend,
};
use crate::workspace_overlay::stores::kv_store::KvWorkspaceStore;

#[path = "group_commit_tests.rs"]
mod group_commit_tests;

#[path = "namespace_permission_tests.rs"]
mod namespace_permission_tests;

const IDLE: u8 = 0;
const RENEW_ONCE: u8 = 1;
const ALWAYS_BUSY: u8 = 2;
const FENCED: u8 = 3;
const BACKEND_ERROR: u8 = 4;
const AUTH_RENEW_ONCE: u8 = 5;
const AUTH_ALWAYS_BUSY: u8 = 6;
const AUTH_FENCED: u8 = 7;
const AUTH_BACKEND_ERROR: u8 = 8;
const MOUNT_RENEW_HEAD_ONCE: u8 = 9;
const MOUNT_RENEW_ALWAYS_BUSY: u8 = 10;
const MOUNT_RENEW_FENCED: u8 = 11;
const MOUNT_RENEW_BACKEND: u8 = 12;
const MOUNT_RENEW_PREP_BUSY_ONCE: u8 = 13;
const MOUNT_RENEW_PREP_ALWAYS_BUSY: u8 = 14;
const MOUNT_RENEW_PREP_FENCED: u8 = 15;
const MOUNT_RENEW_PREP_BACKEND: u8 = 16;
const MOUNT_RENEW_LOST_RESPONSE: u8 = 17;
const MOUNT_RENEW_COMMIT_BUSY: u8 = 18;
const MOUNT_RENEW_BACKEND_MESSAGE: &str = "mounted renewal injected backend response";
const AUTH_BACKEND_MESSAGE: &str = "binding authentication injected backend response";
const BACKEND_MESSAGE: &str = "record_open preparation injected backend response";

struct ValidationControl {
    group_commit: group_commit_tests::Control,
    mode: AtomicU8,
    target: AtomicUsize,
    calls: AtomicUsize,
    conflicts: AtomicUsize,
    packets: Mutex<Vec<Vec<KvCheck>>>,
    deadlines: Mutex<Vec<i64>>,
    binding_reads: Mutex<Vec<Vec<Vec<u8>>>>,
    renewal_routes: AtomicUsize,
    /// Every authenticated/consistent backend round trip (frozen-base proof).
    validations: AtomicUsize,
    reached: Semaphore,
    resume: Semaphore,
}

impl ValidationControl {
    fn new() -> Self {
        Self {
            group_commit: group_commit_tests::Control::new(),
            mode: AtomicU8::new(IDLE),
            target: AtomicUsize::new(1),
            calls: AtomicUsize::new(0),
            conflicts: AtomicUsize::new(0),
            packets: Mutex::new(Vec::new()),
            deadlines: Mutex::new(Vec::new()),
            binding_reads: Mutex::new(Vec::new()),
            renewal_routes: AtomicUsize::new(0),
            validations: AtomicUsize::new(0),
            reached: Semaphore::new(0),
            resume: Semaphore::new(0),
        }
    }

    fn arm(&self, mode: u8, target: usize) {
        assert_eq!(self.calls.load(Ordering::SeqCst), 0);
        self.target.store(target, Ordering::SeqCst);
        self.mode.store(mode, Ordering::SeqCst);
    }
}

struct ValidationBackend<B> {
    inner: Arc<B>,
    control: Arc<ValidationControl>,
}

fn permission_validation(checks: &[KvCheck], writes: &[KvWrite]) -> bool {
    // This is the view_keys packet in kv_store::packed_permissions. Matching
    // its fixed prefix excludes reader-pin validation and renewal CAS calls.
    writes.is_empty()
        && checks.len() >= 11
        && checks[0].key.starts_with(b"ws/")
        && checks[1].key.starts_with(b"layer/")
        && checks[2].key.starts_with(b"layer/")
        && checks[3].key.starts_with(b"lease/")
        && checks[4].key.starts_with(b"packed/v3/current/")
        && checks[8].key.starts_with(b"packed-v3/writer/")
        && checks[9].key.starts_with(b"open/v3/")
        && checks[10].key.starts_with(b"open/v3/recovery/")
}

fn binding_authentication(checks: &[KvCheck]) -> bool {
    // load_packed_binding_record's version-one fixture packet, followed by
    // the real registry checks. Renewal and reader-pin packets have other keys.
    checks.len() >= 7
        && checks[0].key.starts_with(b"ws/")
        && checks[1].key.starts_with(b"layer/")
        && checks[2].key.starts_with(b"lease/")
        && checks[3].key.starts_with(b"packed/v3/current/")
        && checks[4].key.starts_with(b"packed/v3/claim/")
        && checks[5].key.starts_with(b"packed/v3/history/")
        && checks[6].key.starts_with(b"layer/")
}

fn mounted_renewal_mode(mode: u8) -> bool {
    matches!(
        mode,
        MOUNT_RENEW_HEAD_ONCE
            | MOUNT_RENEW_ALWAYS_BUSY
            | MOUNT_RENEW_FENCED
            | MOUNT_RENEW_BACKEND
            | MOUNT_RENEW_PREP_BUSY_ONCE
            | MOUNT_RENEW_PREP_ALWAYS_BUSY
            | MOUNT_RENEW_PREP_FENCED
            | MOUNT_RENEW_PREP_BACKEND
            | MOUNT_RENEW_LOST_RESPONSE
            | MOUNT_RENEW_COMMIT_BUSY
    )
}

fn mounted_renewal_packet(checks: &[KvCheck], writes: &[KvWrite]) -> bool {
    // Actual joint renewal writes both native lease and open expiry. The
    // nonempty inode mutation below writes neither, so it cannot be paused.
    writes.iter().any(|write| {
        matches!(write, KvWrite::Put { key, .. }
        if key.starts_with(b"lease/"))
    }) && writes.iter().any(|write| {
        matches!(write, KvWrite::Put { key, .. }
            if key.starts_with(b"open/v3/") && !key.starts_with(b"open/v3/recovery/"))
    }) && checks
        .iter()
        .any(|check| check.key.starts_with(b"packed-v3/writer/"))
        && checks
            .iter()
            .any(|check| check.key.starts_with(b"packed/v3/current/"))
}

#[async_trait]
impl<B: WorkspaceKvBackend> WorkspaceKvBackend for ValidationBackend<B> {
    fn name(&self) -> &'static str {
        "record-open-preparation-controlled-kv"
    }
    fn supports_consistent_reads(&self) -> bool {
        self.inner.supports_consistent_reads()
    }
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        self.inner.get(key).await
    }
    async fn get_many_consistent(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        self.control
            .group_commit
            .read_snapshot(self.inner.as_ref(), keys)
            .await
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
        self.control.validations.fetch_add(1, Ordering::SeqCst);
        let mode = self.control.mode.load(Ordering::SeqCst);
        if mounted_renewal_mode(mode)
            && keys.len() == 2
            && keys[0].starts_with(b"packed/v3/current/")
            && keys[1].starts_with(b"packed-v3/writer/")
        {
            let ordinal = self.control.renewal_routes.fetch_add(1, Ordering::SeqCst) + 1;
            match mode {
                MOUNT_RENEW_PREP_BUSY_ONCE if ordinal == 1 => return Err(WorkspaceError::Busy),
                MOUNT_RENEW_PREP_ALWAYS_BUSY => return Err(WorkspaceError::Busy),
                MOUNT_RENEW_PREP_FENCED => return Err(WorkspaceError::Fenced),
                MOUNT_RENEW_PREP_BACKEND => {
                    return Err(WorkspaceError::Backend(MOUNT_RENEW_BACKEND_MESSAGE.into()));
                }
                _ => {}
            }
        }
        if matches!(
            self.control.mode.load(Ordering::SeqCst),
            AUTH_RENEW_ONCE | AUTH_ALWAYS_BUSY | AUTH_FENCED | AUTH_BACKEND_ERROR
        ) && keys.len() >= 6
            && keys[0].starts_with(b"ws/")
            && keys[1].starts_with(b"layer/")
            && keys[2].starts_with(b"lease/")
            && keys[3].starts_with(b"packed/v3/current/")
            && keys[4].starts_with(b"packed/v3/claim/")
            && keys[5].starts_with(b"packed/v3/history/")
        {
            self.control.binding_reads.lock().await.push(keys.to_vec());
        }
        self.inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await
    }
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.inner.scan_prefix(prefix).await
    }
    async fn scan_prefix_with_byte_limits(
        &self,
        prefix: &[u8],
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        // This fixture's ClockBackend walks its BTree prefix in one bounded
        // page. Prevent its request cap from shortening the requested window;
        // reader-slot admission includes its own overflow sentinel row.
        limits.validate()?;
        if limits.max_data_requests < limits.max_records {
            return Err(WorkspaceError::InvalidReadPlan(
                "controlled clock fixture requires a complete prefix window".into(),
            ));
        }
        self.inner
            .scan_prefix_page_with_byte_limits(prefix, None, limits)
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
        self.inner.compare_and_swap(checks, writes).await
    }
    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        expires_at_ns: i64,
    ) -> Result<bool, WorkspaceError> {
        self.control.group_commit.before_commit(writes).await;
        self.control.validations.fetch_add(1, Ordering::SeqCst);
        let mode = self.control.mode.load(Ordering::SeqCst);
        let renewal_tracked = mounted_renewal_mode(mode) && mounted_renewal_packet(checks, writes);
        if renewal_tracked {
            let ordinal = self.control.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.control.packets.lock().await.push(checks.to_vec());
            self.control.deadlines.lock().await.push(expires_at_ns);
            match mode {
                MOUNT_RENEW_HEAD_ONCE if ordinal == 1 => {
                    self.control.reached.add_permits(1);
                    self.control.resume.acquire().await.unwrap().forget();
                }
                MOUNT_RENEW_ALWAYS_BUSY => return Ok(false),
                MOUNT_RENEW_COMMIT_BUSY => return Err(WorkspaceError::Busy),
                MOUNT_RENEW_FENCED => return Err(WorkspaceError::Fenced),
                MOUNT_RENEW_BACKEND => {
                    return Err(WorkspaceError::Backend(MOUNT_RENEW_BACKEND_MESSAGE.into()));
                }
                _ => {}
            }
        }
        let tracked = permission_validation(checks, writes)
            && matches!(
                self.control.mode.load(Ordering::SeqCst),
                RENEW_ONCE | ALWAYS_BUSY | FENCED | BACKEND_ERROR
            );
        if tracked {
            let ordinal = self.control.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.control.packets.lock().await.push(checks.to_vec());
            match self.control.mode.load(Ordering::SeqCst) {
                RENEW_ONCE if ordinal == self.control.target.load(Ordering::SeqCst) => {
                    self.control.reached.add_permits(1);
                    self.control.resume.acquire().await.unwrap().forget();
                }
                ALWAYS_BUSY => return Ok(false),
                FENCED => return Err(WorkspaceError::Fenced),
                BACKEND_ERROR => return Err(WorkspaceError::Backend(BACKEND_MESSAGE.into())),
                _ => {}
            }
        }
        let result = self
            .inner
            .compare_and_swap_before(checks, writes, expires_at_ns)
            .await;
        if (tracked || renewal_tracked) && matches!(result, Ok(false)) {
            self.control.conflicts.fetch_add(1, Ordering::SeqCst);
        }
        if renewal_tracked && mode == MOUNT_RENEW_LOST_RESPONSE && matches!(result, Ok(true)) {
            return Err(WorkspaceError::Backend(MOUNT_RENEW_BACKEND_MESSAGE.into()));
        }
        result
    }
    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        expires_at_ns: i64,
        limits: KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        self.control.validations.fetch_add(1, Ordering::SeqCst);
        let tracked = binding_authentication(checks)
            && matches!(
                self.control.mode.load(Ordering::SeqCst),
                AUTH_RENEW_ONCE | AUTH_ALWAYS_BUSY | AUTH_FENCED | AUTH_BACKEND_ERROR
            );
        if tracked {
            let ordinal = self.control.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.control.packets.lock().await.push(checks.to_vec());
            self.control.deadlines.lock().await.push(expires_at_ns);
            match self.control.mode.load(Ordering::SeqCst) {
                AUTH_RENEW_ONCE if ordinal == self.control.target.load(Ordering::SeqCst) => {
                    self.control.reached.add_permits(1);
                    self.control.resume.acquire().await.unwrap().forget();
                }
                AUTH_ALWAYS_BUSY => return Ok(false),
                AUTH_FENCED => return Err(WorkspaceError::Fenced),
                AUTH_BACKEND_ERROR => {
                    return Err(WorkspaceError::Backend(AUTH_BACKEND_MESSAGE.into()));
                }
                _ => {}
            }
        }
        let result = self
            .inner
            .authenticate_checks_before_bounded(checks, expires_at_ns, limits)
            .await;
        if tracked && matches!(result, Ok(false)) {
            self.control.conflicts.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
    }
}

struct UnusedUpper;

#[async_trait]
impl BlockStore for UnusedUpper {
    async fn write_fresh_range(
        &self,
        _key: BlockKey,
        _offset: u64,
        _bytes: &[u8],
    ) -> anyhow::Result<u64> {
        anyhow::bail!("record_open must not write payload")
    }
    async fn read_range(
        &self,
        _key: BlockKey,
        _offset: u64,
        _bytes: &mut [u8],
    ) -> anyhow::Result<()> {
        anyhow::bail!("record_open must not read upper payload")
    }
    async fn delete_range(&self, _key: BlockKey, _count: u64) -> anyhow::Result<()> {
        anyhow::bail!("record_open must not delete payload")
    }
}

struct Fixture<B: WorkspaceKvBackend> {
    _objects: tempfile::TempDir,
    lower: Arc<PackedV3ReadonlyMeta<crate::cadapter::localfs::LocalFsBackend>>,
    meta: WorkspaceMetaLayer<KvWorkspaceStore<ValidationBackend<B>>>,
    inner: Arc<B>,
    control: Arc<ValidationControl>,
    authority: Arc<dyn PackedMountAuthority>,
}

async fn fixture() -> Fixture<impl WorkspaceKvBackend> {
    fixture_with().await
}

async fn fixture_with() -> Fixture<impl WorkspaceKvBackend> {
    fixture_with_cache(0).await
}

/// `metadata_bytes` > 0 enables the lower index cache and immutable memos,
/// as the CLI mount does.
async fn fixture_with_cache(metadata_bytes: u64) -> Fixture<impl WorkspaceKvBackend> {
    fixture_with_backend(
        metadata_bytes,
        Arc::new(packed_open_preparation_test_backend()),
    )
    .await
}

async fn fixture_with_backend<B: WorkspaceKvBackend>(
    metadata_bytes: u64,
    inner: Arc<B>,
) -> Fixture<B> {
    let (objects, client, snapshot, proof, _payload) = packed().await;
    let lower = Arc::new(PackedV3ReadonlyMeta::from_v3(
        client,
        snapshot,
        4096,
        metadata_bytes,
    ));
    let control = Arc::new(ValidationControl::new());
    let store = Arc::new(
        KvWorkspaceStore::new(ValidationBackend {
            inner: inner.clone(),
            control: control.clone(),
        })
        .with_packed_reader_pin_budget(lower.mount_budget()),
    );
    let install = request(store.as_ref(), proof).await;
    store
        .install_packed_lower_binding(install.clone())
        .await
        .unwrap();
    store
        .release_lease(ReleaseLease {
            lease_id: install.guard.lease_id,
            holder_generation: install.guard.holder_generation,
        })
        .await
        .unwrap();
    let authority = store
        .clone()
        .acquire_packed_mount_if_present(PackedMountSessionRequest {
            workspace_id: install.guard.workspace_id,
            holder_generation: 8,
            ttl_ns: 30_000_000_000,
            operator_managed: false,
            budget: lower.mount_budget(),
        })
        .await
        .unwrap()
        .expect("actual packed mount authority");
    let lower_handle = lower.clone();
    let meta = WorkspaceMetaLayer::with_chunk_size(store, authority.view(), 4096)
        .with_packed_v3_lower_from_store(
            lower,
            Arc::new(UnusedUpper),
            ChunkLayout {
                chunk_size: 4096,
                block_size: 4096,
            },
        )
        .await
        .unwrap();
    meta.initialize().await.unwrap();
    Fixture {
        _objects: objects,
        lower: lower_handle,
        meta,
        inner,
        control,
        authority,
    }
}

async fn shutdown<B: WorkspaceKvBackend>(fixture: &Fixture<B>) {
    fixture.control.mode.store(IDLE, Ordering::SeqCst);
    fixture.authority.close_renewals_and_drain().await.unwrap();
    fixture.meta.shutdown_session().await.unwrap();
}

fn observed_native_record<T: serde::de::DeserializeOwned>(raw: &Option<Vec<u8>>) -> T {
    bincode::deserialize(
        raw.as_deref()
            .unwrap()
            .strip_prefix(b"BWSKV001")
            .expect("real native row must retain its KV envelope"),
    )
    .unwrap()
}

async fn real_single_inode_permission_mutation<B: WorkspaceKvBackend>(f: &Fixture<B>) {
    let view = f.authority.view();
    let guard = HeadGuard {
        workspace_id: view.workspace_id,
        expected_head_layer_id: view.head_layer_id,
        expected_head_epoch: view.head_epoch,
        lease_id: view.lease_id,
        holder_generation: view.holder_generation,
    };
    let store = f.meta.store();
    let binding = store
        .load_packed_lower_binding(guard.clone())
        .await
        .unwrap()
        .unwrap();
    let snapshot = store
        .read_packed_permission_snapshot(
            guard.clone(),
            binding.clone(),
            PermissionSnapshotQuery {
                layer_ids: [guard.expected_head_layer_id, binding.base_layer_id],
                inodes: vec![1],
                dentry: None,
            },
        )
        .await
        .unwrap();
    let first = snapshot.layers[0].next_sequence;
    let mut inode = snapshot
        .inodes
        .into_iter()
        .find(|row| row.ino == 1)
        .unwrap();
    inode.layer_id = guard.expected_head_layer_id;
    inode.mode = (inode.mode & !0o777) | 0o700;
    let mut mutation = VersionedMutation::empty(guard, snapshot.layers, 4096);
    mutation.inodes.push(inode);
    assert_eq!(mutation.validate().unwrap(), 1);
    let result = store
        .apply_packed_versioned_mutation(mutation, binding)
        .await
        .unwrap();
    assert_eq!(
        (result.first_sequence, result.last_sequence),
        (Some(first), Some(first))
    );
}

#[tokio::test]
async fn packed_mount_renewal_restarts_after_real_head_permission_mutation() {
    let f = fixture().await;
    f.meta.stat(400).await.unwrap().unwrap();
    let budget = f.authority.budget().clone();
    let baseline = budget.state().used;
    let view = f.authority.view();
    let head_key = format!("layer/{}", view.head_layer_id).into_bytes();
    let row_keys = vec![
        format!("lease/{}/{}", view.workspace_id, view.lease_id).into_bytes(),
        format!("open/v3/{}", view.workspace_id).into_bytes(),
        head_key.clone(),
    ];
    let before = f.inner.get_many_consistent(&row_keys).await.unwrap();
    f.control.arm(MOUNT_RENEW_HEAD_ONCE, 1);
    let mutating = async {
        f.control.reached.acquire().await.unwrap().forget();
        let captured = f.control.packets.lock().await[0].clone();
        real_single_inode_permission_mutation(&f).await;
        let keys = captured
            .iter()
            .map(|check| check.key.clone())
            .collect::<Vec<_>>();
        let after_mutation = f.inner.get_many_consistent(&keys).await.unwrap();
        for (check, row) in captured.iter().zip(&after_mutation) {
            if check.key == head_key {
                let before_head: LayerRecord = observed_native_record(&check.expected);
                let mut after_head: LayerRecord = observed_native_record(row);
                assert_eq!(after_head.next_sequence, before_head.next_sequence + 1);
                after_head.next_sequence = before_head.next_sequence;
                assert_eq!(
                    after_head, before_head,
                    "only normal head sequence must change"
                );
            } else {
                assert_eq!(
                    row, &check.expected,
                    "all other captured authorities stay identical"
                );
            }
        }
        let rows = f.inner.get_many_consistent(&row_keys).await.unwrap();
        assert_eq!(
            &rows[..2],
            &before[..2],
            "mutation must not renew or replace lease/open"
        );
        assert_ne!(
            rows[2], before[2],
            "nonempty mutation must really advance head bytes"
        );
        f.control.resume.add_permits(1);
        rows
    };
    let (renewed, after_mutation) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(f.authority.clone().renew(60_000_000_000), mutating)
    })
    .await
    .expect("actual renewal/head mutation race must reach terminal");
    assert_eq!(
        f.control.conflicts.load(Ordering::SeqCst),
        1,
        "real delegated CAS must conflict"
    );
    // RED on prior renew_owned: normal changed head bytes were returned as Fenced.
    assert!(
        renewed.is_ok(),
        "normal head mutation is not a holder fence: {renewed:?}"
    );
    assert_eq!(f.control.calls.load(Ordering::SeqCst), 2);
    assert_eq!(f.control.renewal_routes.load(Ordering::SeqCst), 2);
    let after_renewal = f.inner.get_many_consistent(&row_keys).await.unwrap();
    assert_ne!(after_renewal[0], before[0]);
    assert_ne!(after_renewal[1], before[1]);
    assert_eq!(
        after_renewal[2], after_mutation[2],
        "renewal must preserve the committed head sequence"
    );
    assert_eq!(f.authority.view(), view);
    let packets = f.control.packets.lock().await;
    assert_eq!(
        packets[0]
            .iter()
            .map(|check| &check.key)
            .collect::<Vec<_>>(),
        packets[1]
            .iter()
            .map(|check| &check.key)
            .collect::<Vec<_>>()
    );
    let fresh_head = packets[1]
        .iter()
        .find(|check| check.key == head_key)
        .unwrap();
    assert_eq!(fresh_head.expected, after_mutation[2]);
    drop(packets);
    let deadlines = f.control.deadlines.lock().await;
    assert_eq!(
        deadlines.as_slice(),
        &[observed_lease_expiry(&before[0]); 2]
    );
    drop(deadlines);
    // A later operation must use the actual renewed lease deadline, rather than
    // the authority's construction-time lease snapshot or its proposed expiry.
    f.authority.clone().renew(90_000_000_000).await.unwrap();
    assert_eq!(f.control.calls.load(Ordering::SeqCst), 3);
    assert_eq!(f.control.renewal_routes.load(Ordering::SeqCst), 3);
    assert_eq!(
        f.control.deadlines.lock().await[2],
        observed_lease_expiry(&after_renewal[0])
    );
    assert!(f.meta.open_counts.is_empty());
    assert_eq!(budget.state().used, baseline);
    shutdown(&f).await;
    drop(f);
    assert_eq!(budget.state().used, [0; 8]);
}

#[tokio::test]
async fn packed_mount_renewal_restarts_after_preparation_busy() {
    let f = fixture().await;
    let budget = f.authority.budget().clone();
    let baseline = budget.state().used;
    f.control.arm(MOUNT_RENEW_PREP_BUSY_ONCE, 1);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        f.authority.clone().renew(60_000_000_000),
    )
    .await
    .unwrap();
    assert!(
        result.is_ok(),
        "definite preparation Busy must rebuild: {result:?}"
    );
    assert_eq!(f.control.renewal_routes.load(Ordering::SeqCst), 2);
    assert_eq!(f.control.calls.load(Ordering::SeqCst), 1);
    assert_eq!(budget.state().used, baseline);
    shutdown(&f).await;
    drop(f);
    assert_eq!(budget.state().used, [0; 8]);
}

#[tokio::test]
async fn packed_mount_renewal_busy_exhausts_exactly_cas_cap() {
    for mode in [MOUNT_RENEW_PREP_ALWAYS_BUSY, MOUNT_RENEW_ALWAYS_BUSY] {
        let f = fixture().await;
        let budget = f.authority.budget().clone();
        let baseline = budget.state().used;
        f.control.arm(mode, 1);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            f.authority.clone().renew(60_000_000_000),
        )
        .await
        .expect("renewal retry cap must terminate");
        assert!(
            matches!(result, Err(WorkspaceError::Busy)),
            "exhausted result: {result:?}"
        );
        // The bounded contention budget is the store's CAS_MAX_RETRIES (256).
        assert_eq!(f.control.renewal_routes.load(Ordering::SeqCst), 256);
        assert_eq!(
            f.control.calls.load(Ordering::SeqCst),
            if mode == MOUNT_RENEW_ALWAYS_BUSY {
                256
            } else {
                0
            }
        );
        assert!(f.meta.open_counts.is_empty());
        assert_eq!(budget.state().used, baseline);
        shutdown(&f).await;
        drop(f);
        assert_eq!(budget.state().used, [0; 8]);
    }
}

#[tokio::test]
async fn packed_mount_renewal_does_not_replay_fatal_or_unknown_errors() {
    for mode in [
        MOUNT_RENEW_PREP_FENCED,
        MOUNT_RENEW_PREP_BACKEND,
        MOUNT_RENEW_FENCED,
        MOUNT_RENEW_BACKEND,
        MOUNT_RENEW_COMMIT_BUSY,
    ] {
        let f = fixture().await;
        let budget = f.authority.budget().clone();
        let baseline = budget.state().used;
        f.control.arm(mode, 1);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            f.authority.clone().renew(60_000_000_000),
        )
        .await
        .expect("fatal renewal errors must return directly");
        if matches!(mode, MOUNT_RENEW_PREP_FENCED | MOUNT_RENEW_FENCED) {
            assert!(matches!(result, Err(WorkspaceError::Fenced)));
        } else if mode == MOUNT_RENEW_COMMIT_BUSY {
            assert!(matches!(result, Err(WorkspaceError::Busy)));
        } else {
            assert!(matches!(result, Err(WorkspaceError::Backend(ref message))
                if message == MOUNT_RENEW_BACKEND_MESSAGE));
        }
        assert_eq!(f.control.renewal_routes.load(Ordering::SeqCst), 1);
        assert_eq!(
            f.control.calls.load(Ordering::SeqCst),
            if matches!(
                mode,
                MOUNT_RENEW_FENCED | MOUNT_RENEW_BACKEND | MOUNT_RENEW_COMMIT_BUSY
            ) {
                1
            } else {
                0
            }
        );
        assert_eq!(budget.state().used, baseline);
        shutdown(&f).await;
        drop(f);
        assert_eq!(budget.state().used, [0; 8]);
    }
}

#[tokio::test]
async fn packed_mount_renewal_confirms_only_the_exact_lost_response_successor() {
    let f = fixture().await;
    let budget = f.authority.budget().clone();
    let baseline = budget.state().used;
    f.control.arm(MOUNT_RENEW_LOST_RESPONSE, 1);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        f.authority.clone().renew(60_000_000_000),
    )
    .await
    .unwrap();
    assert!(
        result.is_ok(),
        "actual committed successor must be confirmed: {result:?}"
    );
    assert_eq!(
        f.control.calls.load(Ordering::SeqCst),
        1,
        "no new renewal mutation may be submitted"
    );
    assert_eq!(
        f.control.renewal_routes.load(Ordering::SeqCst),
        1,
        "unknown response must not rebuild authority"
    );
    assert_eq!(f.control.conflicts.load(Ordering::SeqCst), 0);
    assert_eq!(budget.state().used, baseline);
    shutdown(&f).await;
    drop(f);
    assert_eq!(budget.state().used, [0; 8]);
}

fn observed_lease_expiry(raw: &Option<Vec<u8>>) -> i64 {
    let payload = raw
        .as_deref()
        .unwrap()
        .strip_prefix(b"BWSKV001")
        .expect("actual native lease uses the KV envelope");
    bincode::deserialize::<SnapshotLease>(payload)
        .unwrap()
        .expires_at_ns
}

async fn read_only_round_trips<B: WorkspaceKvBackend>(f: &Fixture<B>) -> usize {
    let before = f.control.validations.load(Ordering::SeqCst);
    let attr = f.meta.stat(400).await.unwrap().unwrap();
    f.meta.lookup_with_attr_bytes(1, b"nonzero").await.unwrap();
    f.meta.inode_permissions(400).await.unwrap().unwrap();
    f.meta
        .record_open(400, attr, true, false, false)
        .await
        .unwrap();
    f.meta.record_close(400).await.unwrap();
    f.control.validations.load(Ordering::SeqCst) - before
}

#[tokio::test]
async fn frozen_base_read_path_does_no_per_operation_lower_validation() {
    let frozen = fixture_with().await;
    let frozen_trips = read_only_round_trips(&frozen).await;
    // Frozen base: no binding/pin/fence authentication on stat, lookup,
    // permission reads or a read-only open; native rows use plain MGETs.
    assert_eq!(
        frozen_trips, 0,
        "frozen base must not re-authenticate lower"
    );
    shutdown(&frozen).await;
}

#[tokio::test]
async fn frozen_lower_permission_and_lookup_inputs_are_fetched_once_per_binding() {
    use crate::meta::layer::MetaLayer;
    let f = fixture_with_cache(64 << 20).await;
    // First pass loads the immutable lower once.
    let first_permissions = f.meta.inode_permissions(400).await.unwrap().unwrap();
    let first_lookup = f
        .meta
        .lookup_with_attr_bytes(1, b"nonzero")
        .await
        .unwrap()
        .unwrap();
    let first_paths = f.meta.get_paths_bytes(400).await.unwrap();
    let after_first = f.lower.runtime_range_gets();
    // Drop every upper-side cache: only the binding-lifetime lower memos may
    // answer now, and no lower object is fetched again.
    f.meta.invalidate_upper_rows();
    for _ in 0..3 {
        let permissions = f.meta.inode_permissions(400).await.unwrap().unwrap();
        assert_eq!(permissions.attr.mode, first_permissions.attr.mode);
        assert_eq!(permissions.attr.uid, first_permissions.attr.uid);
        assert_eq!(permissions.access_acl, first_permissions.access_acl);
        let lookup = f
            .meta
            .lookup_with_attr_bytes(1, b"nonzero")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lookup.0, first_lookup.0);
        assert_eq!(f.meta.get_paths_bytes(400).await.unwrap(), first_paths);
        f.meta.invalidate_upper_rows();
    }
    assert_eq!(
        f.lower.runtime_range_gets(),
        after_first,
        "frozen lower inputs must be served from the binding-lifetime memo"
    );
    assert_eq!(first_paths, vec![b"/nonzero".to_vec()]);
    // An upper change still shadows the cached lower inputs (copy-up chmod).
    f.meta
        .set_attr(
            400,
            &crate::meta::store::SetAttrRequest {
                mode: Some(0o600),
                ..Default::default()
            },
            crate::meta::store::SetAttrFlags::empty(),
        )
        .await
        .unwrap();
    let changed = f.meta.inode_permissions(400).await.unwrap().unwrap();
    assert_eq!(changed.attr.mode & 0o777, 0o600);
    assert_ne!(first_permissions.attr.mode & 0o777, 0o600);
    shutdown(&f).await;
}

#[tokio::test]
async fn creators_first_write_open_skips_the_empty_cas_but_later_opens_commit_it() {
    use crate::meta::layer::{NamespaceActor, scope_created_inode_open, scope_namespace_actor};
    {
        let f = fixture_with().await;
        let attr = f.meta.stat(400).await.unwrap().unwrap();
        let actor = NamespaceActor {
            uid: 0,
            gid: 0,
            groups: Vec::new(),
        };
        // The creator's own first open (create just committed this inode).
        let before = f.control.validations.load(Ordering::SeqCst);
        scope_namespace_actor(
            Some(actor.clone()),
            scope_created_inode_open(
                400,
                true,
                f.meta.record_open(400, attr.clone(), true, true, false),
            ),
        )
        .await
        .unwrap();
        let after_created = f.control.validations.load(Ordering::SeqCst);
        assert_eq!(after_created, before, "creator's first open: no CAS");
        assert_eq!(f.meta.open_counts.get(&400).map(|count| *count), Some(1));
        // Any other writable open still commits its authenticated CAS.
        scope_namespace_actor(
            Some(actor),
            f.meta.record_open(400, attr, true, true, false),
        )
        .await
        .unwrap();
        assert!(f.control.validations.load(Ordering::SeqCst) > after_created);
        f.meta.record_close(400).await.unwrap();
        f.meta.record_close(400).await.unwrap();
        assert!(f.meta.open_counts.is_empty());
        shutdown(&f).await;
    }
}

#[tokio::test]
async fn frozen_base_write_open_still_authenticates_in_cas() {
    let f = fixture_with().await;
    let attr = f.meta.stat(400).await.unwrap().unwrap();
    let before = f.control.validations.load(Ordering::SeqCst);
    f.meta
        .record_open(400, attr, true, true, false)
        .await
        .unwrap();
    assert!(
        f.control.validations.load(Ordering::SeqCst) > before,
        "write open must commit its authenticated CAS"
    );
    f.meta.record_close(400).await.unwrap();
    shutdown(&f).await;
}
