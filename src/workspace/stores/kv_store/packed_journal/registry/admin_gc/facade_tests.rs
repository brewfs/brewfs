use super::*;
use crate::workspace::model::BaseRevision;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
pub(super) struct Backend {
    pub(super) rows: tokio::sync::Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    auth: AtomicUsize,
    denied: AtomicBool,
    scans: AtomicUsize,
    reads: AtomicUsize,
    shutdowns: AtomicUsize,
    object_deletes: Arc<AtomicUsize>,
    block_scan: AtomicBool,
    scan_started: tokio::sync::Notify,
    release_scan: tokio::sync::Notify,
}
#[async_trait]
impl WorkspaceKvBackend for Backend {
    fn name(&self) -> &'static str {
        "gc-contract-test"
    }
    fn supports_consistent_reads(&self) -> bool {
        true
    }
    async fn authenticate_gc_admin(&self) -> Result<(), WorkspaceError> {
        self.auth.fetch_add(1, Ordering::SeqCst);
        if self.denied.load(Ordering::SeqCst) {
            return Err(WorkspaceError::UnsupportedCapability(
                "test enforced identity denied",
            ));
        }
        Ok(())
    }
    async fn shutdown_metadata_backend(&self) -> Result<(), WorkspaceError> {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        panic!("unbounded GET")
    }
    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        limits.validate_keys(keys)?;
        self.reads.fetch_add(1, Ordering::SeqCst);
        let rows = self.rows.lock().await;
        let values = keys
            .iter()
            .map(|key| rows.get(key).cloned())
            .collect::<Vec<_>>();
        if values
            .iter()
            .flatten()
            .any(|raw| raw.len() > limits.max_value_bytes)
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok((values, 1_000_000_000))
    }
    async fn scan_prefix(&self, _: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        panic!("unbounded SCAN")
    }
    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        limits.validate_scan_page(prefix, after)?;
        self.scans.fetch_add(1, Ordering::SeqCst);
        if self.block_scan.load(Ordering::SeqCst) {
            self.scan_started.notify_one();
            self.release_scan.notified().await;
        }
        Ok(self
            .rows
            .lock()
            .await
            .iter()
            .filter(|(key, _)| {
                key.starts_with(prefix) && after.is_none_or(|last| key.as_slice() > last)
            })
            .take(limits.max_records)
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
        let mut rows = self.rows.lock().await;
        if checks
            .iter()
            .any(|check| rows.get(&check.key) != check.expected.as_ref())
        {
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
        Ok(true)
    }
    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        Ok(1_000_000_000)
    }
}

#[derive(Clone, Default)]
pub(super) struct Objects {
    pub(super) deletes: Arc<AtomicUsize>,
    pub(super) fail_delete: Arc<AtomicBool>,
}
#[async_trait]
impl ObjectBackend for Objects {
    fn forbids_mutation_replay(&self) -> bool {
        true
    }
    async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        panic!("GC must not PUT objects")
    }
    async fn get_object(&self, _: &str) -> anyhow::Result<Option<Vec<u8>>> {
        panic!("GC must not whole-GET objects")
    }
    async fn get_object_range(&self, _: &str, _: u64, _: &mut [u8]) -> anyhow::Result<usize> {
        panic!("GC must not read blocks")
    }
    async fn get_etag(&self, _: &str) -> anyhow::Result<String> {
        panic!("GC must not infer mutation result from ETag")
    }
    async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        if self.fail_delete.load(Ordering::SeqCst) {
            anyhow::bail!("uncertain DELETE reply");
        }
        Ok(())
    }
}

fn policy() -> PackedGcPolicy {
    PackedGcPolicy {
        lease_ttl_seconds: 30,
        grace_seconds: 60,
        max_scans: 1,
        max_operations: 1,
        max_protective_rows: 64,
    }
}
fn request(cursor: PackedGcCursor) -> PackedGcTickRequest {
    PackedGcTickRequest {
        policy: policy(),
        cursor,
        cancel: CancellationToken::new(),
    }
}
fn fixture() -> (
    Arc<Backend>,
    Arc<Handle<Backend, Objects>>,
    Arc<SnapshotMountBudget>,
) {
    let backend = Arc::new(Backend::default());
    let budget = SnapshotMountBudget::defaults();
    let store = Arc::new(
        KvWorkspaceStore::from_arc(backend.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    let objects = Objects {
        deletes: backend.object_deletes.clone(),
        ..Default::default()
    };
    let handle = Arc::new(Handle {
        runtime: Arc::new(Runtime {
            store,
            client: ObjectClient::new(objects),
            budget: budget.clone(),
            layout: ChunkLayout {
                chunk_size: 4 << 20,
                block_size: 1 << 20,
            },
            serial: tokio::sync::Mutex::new(()),
            closed: AtomicBool::new(false),
        }),
    });
    (backend, handle, budget)
}
fn released_lease(number: u128) -> SnapshotLease {
    SnapshotLease {
        lease_id: LeaseId::from_uuid(uuid::Uuid::from_u128(number)),
        workspace_id: WorkspaceId::new(),
        base_revision: BaseRevision {
            layer_id: LayerId::new(),
            sealed_version: 1,
            root_hash: [1; 32],
        },
        holder_generation: 1,
        writable: true,
        state: LeaseState::Released,
        expires_at_ns: 1,
        created_at_ns: 1,
        updated_at_ns: 2,
    }
}

#[test]
fn cursor_rejects_cross_tier_oversize_and_unknown_fields() {
    assert!(PackedGcCursor::decode(&vec![b' '; 1025]).is_err());
    assert!(PackedGcCursor::decode(br#"{"tier":"History","after":[1],"authority":true}"#).is_err());
    assert!(
        PackedGcCursor {
            tier: Tier::Lease,
            after: Some(b"packed/journal/wrong".to_vec())
        }
        .encode()
        .is_err()
    );
    assert!(
        PackedGcCursor {
            tier: Tier::Native,
            after: Some(b"x".to_vec())
        }
        .encode()
        .is_err()
    );
    assert!(PackedGcCursor::decode(&PackedGcCursor::default().encode().unwrap()).is_ok());
}

#[tokio::test]
async fn one_row_quota_keeps_deep_cursor_until_actual_empty_page() {
    let (backend, handle, _) = fixture();
    let mut keys = Vec::new();
    for number in 1..=3 {
        let lease = released_lease(number);
        let key = hot_lease_key(lease.workspace_id, lease.lease_id);
        backend
            .rows
            .lock()
            .await
            .insert(key.clone(), encode(&lease).unwrap());
        keys.push(key);
    }
    let mut cursor = PackedGcCursor {
        tier: Tier::Lease,
        after: None,
    };
    for key in keys {
        let report = handle.tick(request(cursor)).await.unwrap();
        assert_eq!(report.scanned, 1);
        assert_eq!(report.attempted, 0);
        assert!(matches!(report.next_cursor.tier, Tier::Lease));
        assert_eq!(report.next_cursor.after, Some(key));
        cursor = PackedGcCursor::decode(&report.next_cursor.encode().unwrap()).unwrap();
    }
    let report = handle.tick(request(cursor)).await.unwrap();
    assert!(matches!(report.next_cursor.tier, Tier::Journal));
    assert_eq!(backend.scans.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn discovered_row_cannot_substitute_another_lease_identity() {
    let (backend, handle, _) = fixture();
    let lease = released_lease(1);
    backend.rows.lock().await.insert(
        hot_lease_key(lease.workspace_id, LeaseId::new()),
        encode(&lease).unwrap(),
    );
    assert!(
        handle
            .tick(request(PackedGcCursor {
                tier: Tier::Lease,
                after: None
            }))
            .await
            .is_err()
    );
    assert_eq!(backend.object_deletes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn credential_rejection_happens_before_any_discovery_or_delete() {
    let (backend, handle, _) = fixture();
    backend.denied.store(true, Ordering::SeqCst);
    assert!(
        handle
            .tick(request(PackedGcCursor::default()))
            .await
            .is_err()
    );
    assert_eq!(backend.auth.load(Ordering::SeqCst), 1);
    assert_eq!(backend.scans.load(Ordering::SeqCst), 0);
    assert_eq!(backend.reads.load(Ordering::SeqCst), 0);
    assert_eq!(backend.object_deletes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn factory_credential_rejection_never_loads_catalog_header() {
    let (backend, handle, budget) = fixture();
    backend.denied.store(true, Ordering::SeqCst);
    assert!(
        handle
            .runtime
            .store
            .open_packed_gc_admin(
                ObjectClient::new(Objects::default()),
                budget,
                handle.runtime.layout
            )
            .await
            .is_err()
    );
    assert_eq!(backend.auth.load(Ordering::SeqCst), 1);
    assert_eq!(backend.reads.load(Ordering::SeqCst), 0);
    assert_eq!(backend.scans.load(Ordering::SeqCst), 0);
    assert_eq!(backend.object_deletes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn closed_exhausted_and_cancelled_requests_never_authenticate() {
    for mode in 0..3 {
        let (backend, handle, budget) = fixture();
        let req = request(PackedGcCursor::default());
        let owner = if mode == 1 {
            Some(
                budget
                    .admit(&[(SnapshotBudgetPool::Metadata, 256 << 20)])
                    .unwrap(),
            )
        } else {
            None
        };
        if mode == 0 {
            budget.close();
        }
        if mode == 2 {
            req.cancel.cancel();
        }
        assert!(handle.tick(req).await.is_err());
        assert_eq!(backend.auth.load(Ordering::SeqCst), 0);
        assert_eq!(backend.scans.load(Ordering::SeqCst), 0);
        drop(owner);
    }
}

#[tokio::test]
async fn dropped_tick_waiter_retains_owner_and_shutdown_waits_for_transport() {
    let (backend, handle, budget) = fixture();
    backend.block_scan.store(true, Ordering::SeqCst);
    let caller_handle = handle.clone();
    let caller =
        tokio::spawn(async move { caller_handle.tick(request(PackedGcCursor::default())).await });
    backend.scan_started.notified().await;
    caller.abort();
    let _ = caller.await;
    assert!(budget.state().used[SnapshotBudgetPool::Metadata as usize] > 0);
    let shutdown_handle = handle.clone();
    let shutdown = tokio::spawn(async move { shutdown_handle.shutdown().await });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    assert_eq!(backend.shutdowns.load(Ordering::SeqCst), 0);
    backend.release_scan.notify_one();
    shutdown.await.unwrap().unwrap();
    assert_eq!(backend.shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(
        budget.state().used[SnapshotBudgetPool::Metadata as usize],
        0
    );
    assert!(budget.state().closed);
}

#[tokio::test]
async fn factory_rejects_a_foreign_ledger_before_authentication() {
    let (backend, handle, _) = fixture();
    assert!(
        handle
            .runtime
            .store
            .open_packed_gc_admin(
                ObjectClient::new(Objects::default()),
                SnapshotMountBudget::defaults(),
                handle.runtime.layout
            )
            .await
            .is_err()
    );
    assert_eq!(backend.auth.load(Ordering::SeqCst), 0);
}

#[test]
fn policy_never_shortens_actual_default_mount_ttl_or_grace() {
    let mut invalid = policy();
    invalid.lease_ttl_seconds = 29;
    assert!(invalid.validate().is_err());
    let mut invalid = policy();
    invalid.grace_seconds = 29;
    assert!(invalid.validate().is_err());
    let mut invalid = policy();
    invalid.max_operations = 0;
    assert!(invalid.validate().is_err());
    assert!(policy().validate().is_ok());
}
