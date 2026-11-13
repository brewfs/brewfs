use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Mutex;

#[derive(Default)]
struct FakeLeaseState {
    lease: Option<Lease>,
    writes: usize,
    reject_replace: bool,
    response_delay: Duration,
}

#[derive(Clone, Default)]
struct FakeLeaseApi(Arc<Mutex<FakeLeaseState>>);

fn lease(holder: &str, rv: &str) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some(LEASE_NAME.into()),
            uid: Some("lease-incarnation".into()),
            resource_version: Some(rv.into()),
            ..Default::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: Some(holder.into()),
            lease_duration_seconds: Some(LEASE_SECONDS),
            // A stale wall-clock value must not permit an immediate takeover.
            renew_time: Some(MicroTime(chrono::DateTime::from_timestamp(0, 0).unwrap())),
            ..Default::default()
        }),
    }
}

#[async_trait]
impl LeaseApi for FakeLeaseApi {
    async fn get(&self) -> anyhow::Result<Option<Lease>> {
        Ok(self.0.lock().await.lease.clone())
    }

    async fn create(&self, requested: &Lease) -> anyhow::Result<Lease> {
        let (response, delay) = {
            let mut state = self.0.lock().await;
            if state.lease.is_some() {
                bail!("AlreadyExists");
            }
            let mut response = requested.clone();
            response.metadata.uid = Some("lease-incarnation".into());
            response.metadata.resource_version = Some("1".into());
            state.lease = Some(response.clone());
            state.writes += 1;
            (response, state.response_delay)
        };
        sleep(delay).await;
        Ok(response)
    }

    async fn replace(&self, requested: &Lease) -> anyhow::Result<Lease> {
        let (response, delay) = {
            let mut state = self.0.lock().await;
            if state.reject_replace {
                bail!("Conflict");
            }
            let current = state.lease.as_ref().ok_or_else(|| anyhow!("NotFound"))?;
            if current.metadata.resource_version != requested.metadata.resource_version
                || current.metadata.uid != requested.metadata.uid
            {
                bail!("Conflict");
            }
            let rv: u64 = current
                .metadata
                .resource_version
                .as_ref()
                .unwrap()
                .parse()?;
            let mut response = requested.clone();
            response.metadata.resource_version = Some((rv + 1).to_string());
            state.lease = Some(response.clone());
            state.writes += 1;
            (response, state.response_delay)
        };
        // The server has committed already: delaying only the response models
        // a late reply without falsely assuming the Lease write was rolled back.
        sleep(delay).await;
        Ok(response)
    }
}

struct DrainingWork {
    started: AtomicBool,
    cancelled: AtomicBool,
    drained: AtomicBool,
    drain_time: Duration,
}

impl Default for DrainingWork {
    fn default() -> Self {
        Self {
            started: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            drain_time: Duration::from_secs(2),
        }
    }
}

#[async_trait]
impl OwnedWork for DrainingWork {
    async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        self.started.store(true, Ordering::SeqCst);
        cancel.cancelled().await;
        self.cancelled.store(true, Ordering::SeqCst);
        sleep(self.drain_time).await;
        self.drained.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn foreign_claim_waits_full_observed_ttl_despite_old_wall_timestamp() {
    let api = FakeLeaseApi::default();
    api.0.lock().await.lease = Some(lease("peer", "1"));
    let mut election = Election::new(api.clone(), "self".into());
    assert!(election.acquire().await.unwrap().is_none());
    tokio::time::advance(Duration::from_secs(34)).await;
    assert!(election.acquire().await.unwrap().is_none());
    assert_eq!(api.0.lock().await.writes, 0);
    tokio::time::advance(Duration::from_secs(1)).await;
    let claim = election.acquire().await.unwrap().unwrap();
    assert_eq!(
        claim.lease.spec.unwrap().holder_identity.as_deref(),
        Some("self")
    );
    assert_eq!(api.0.lock().await.writes, 1);
}

#[tokio::test(start_paused = true)]
async fn observing_new_resource_version_restarts_foreign_expiry_wait() {
    let api = FakeLeaseApi::default();
    api.0.lock().await.lease = Some(lease("peer", "1"));
    let mut election = Election::new(api.clone(), "self".into());
    assert!(election.acquire().await.unwrap().is_none());
    tokio::time::advance(Duration::from_secs(34)).await;
    api.0.lock().await.lease = Some(lease("peer", "2"));
    assert!(election.acquire().await.unwrap().is_none());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(election.acquire().await.unwrap().is_none());
    assert_eq!(api.0.lock().await.writes, 0);
}

#[tokio::test(start_paused = true)]
async fn lost_lease_cancels_then_awaits_owned_drain() {
    let api = FakeLeaseApi::default();
    let mut election = Election::new(api.clone(), "self".into());
    let claim = election.acquire().await.unwrap().unwrap();
    api.0.lock().await.reject_replace = true;
    let work = DrainingWork::default();
    let started = Instant::now();
    assert!(
        leader_session(&election, claim, &work, &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(work.started.load(Ordering::SeqCst));
    assert!(work.cancelled.load(Ordering::SeqCst));
    assert!(work.drained.load(Ordering::SeqCst));
    assert!(started.elapsed() >= RENEW_EVERY + work.drain_time);
}

#[tokio::test(start_paused = true)]
async fn renewal_committed_but_reply_after_old_deadline_is_not_authority() {
    let api = FakeLeaseApi::default();
    let mut election = Election::new(api.clone(), "self".into());
    let mut claim = election.acquire().await.unwrap().unwrap();
    claim.deadline = Instant::now() + Duration::from_secs(6);
    api.0.lock().await.response_delay = Duration::from_secs(3);
    let work = DrainingWork::default();
    assert!(
        leader_session(&election, claim, &work, &CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(
        api.0.lock().await.writes,
        2,
        "renewal did commit on the server"
    );
    assert!(work.cancelled.load(Ordering::SeqCst));
    assert!(work.drained.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn stale_resource_version_cannot_renew_scheduling_claim() {
    let api = FakeLeaseApi::default();
    let mut election = Election::new(api.clone(), "self".into());
    let claim = election.acquire().await.unwrap().unwrap();
    api.0.lock().await.lease = Some(lease("peer", "2"));
    assert!(election.renew(&claim).await.is_err());
    assert_eq!(api.0.lock().await.writes, 1);
}

#[tokio::test(start_paused = true)]
async fn initial_write_timeout_never_returns_a_claim_even_if_server_committed() {
    let api = FakeLeaseApi::default();
    api.0.lock().await.response_delay = API_TIMEOUT + Duration::from_secs(1);
    let mut election = Election::new(api.clone(), "self".into());
    assert!(election.acquire().await.is_err());
    assert_eq!(api.0.lock().await.writes, 1);
}

fn policy() -> PackedGcPolicy {
    PackedGcPolicy {
        lease_ttl_seconds: 30,
        grace_seconds: 60,
        max_scans: 16,
        max_operations: 1,
        max_protective_rows: 64,
    }
}

#[derive(Default)]
struct FakeAdmin {
    dispatched: AtomicUsize,
    shutdowns: AtomicUsize,
}

#[async_trait]
impl PackedGcAdmin for FakeAdmin {
    async fn tick(
        &self,
        request: PackedGcTickRequest,
    ) -> Result<
        brewfs::workspace::packed_admin::PackedGcTickReport,
        brewfs::workspace::error::WorkspaceError,
    > {
        self.dispatched.fetch_add(1, Ordering::SeqCst);
        Ok(
            brewfs::workspace::packed_admin::PackedGcTickReport {
                next_cursor: request.cursor,
                scanned: 0,
                attempted: 0,
                deferred: 0,
                deleted_objects: 0,
                native_deleted_layers: 0,
            },
        )
    }
    async fn shutdown(&self) -> Result<(), brewfs::workspace::error::WorkspaceError> {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn rejected_admin_factory_cannot_dispatch_gc() {
    let admin = Arc::new(FakeAdmin::default());
    let authentications = AtomicUsize::new(0);
    let prepare = async {
        authentications.fetch_add(1, Ordering::SeqCst);
        if authentications.load(Ordering::SeqCst) > 0 {
            return Err(anyhow!("server-side GC admin role cannot be authenticated"));
        }
        Ok(admin.clone() as Arc<dyn PackedGcAdmin>)
    };
    let result = authenticated_tick(
        prepare,
        policy(),
        PackedGcCursor::default(),
        CancellationToken::new(),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(authentications.load(Ordering::SeqCst), 1);
    assert_eq!(admin.dispatched.load(Ordering::SeqCst), 0);
    assert_eq!(
        admin.shutdowns.load(Ordering::SeqCst),
        0,
        "rejection creates no GC owner"
    );
}

#[tokio::test]
async fn cancelled_scope_dispatches_no_tick_and_still_shuts_down_owner() {
    let admin = Arc::new(FakeAdmin::default());
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(authenticated_tick(
        async { Ok(admin.clone() as Arc<dyn PackedGcAdmin>) },
        policy(),
        PackedGcCursor::default(),
        cancel
    )
    .await
    .unwrap()
    .is_none());
    assert_eq!(admin.dispatched.load(Ordering::SeqCst), 0);
    assert_eq!(admin.shutdowns.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn authenticated_scope_shuts_down_after_one_bounded_tick() {
    let admin = Arc::new(FakeAdmin::default());
    assert!(authenticated_tick(
        async { Ok(admin.clone() as Arc<dyn PackedGcAdmin>) },
        policy(),
        PackedGcCursor::default(),
        CancellationToken::new()
    )
    .await
    .unwrap()
    .is_some());
    assert_eq!(admin.dispatched.load(Ordering::SeqCst), 1);
    assert_eq!(admin.shutdowns.load(Ordering::SeqCst), 1);
}

fn cursor_map(uid: &str, catalog: &str) -> ConfigMap {
    let encoded = String::from_utf8(PackedGcCursor::default().encode().unwrap()).unwrap();
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(cursor_name(uid).unwrap()),
            uid: Some("cursor-incarnation".into()),
            resource_version: Some("1".into()),
            owner_references: Some(vec![
                k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                    api_version: "storage.brewfs.io/v1alpha1".into(),
                    kind: "BrewFSCluster".into(),
                    name: "cluster".into(),
                    uid: uid.into(),
                    controller: Some(false),
                    block_owner_deletion: Some(false),
                },
            ]),
            ..Default::default()
        },
        data: Some(BTreeMap::from([
            (CURSOR_KEY.into(), encoded),
            (CLUSTER_UID_KEY.into(), uid.into()),
            (CATALOG_KEY.into(), catalog.into()),
        ])),
        ..Default::default()
    }
}

#[test]
fn durable_cursor_rejects_cross_cluster_catalog_and_oversized_hints() {
    let uid = Uuid::from_u128(1).to_string();
    let other = Uuid::from_u128(2).to_string();
    let name = cursor_name(&uid).unwrap();
    let map = cursor_map(&uid, "catalog-a");
    let accepted = load_cursor(
        name.clone(),
        uid.clone(),
        "catalog-a".into(),
        Some(map.clone()),
    )
    .unwrap();
    assert_eq!(
        accepted.cursor.encode().unwrap(),
        PackedGcCursor::default().encode().unwrap()
    );
    assert!(load_cursor(
        name.clone(),
        uid.clone(),
        "catalog-b".into(),
        Some(map.clone())
    )
    .is_err());
    assert!(load_cursor(name.clone(), other, "catalog-a".into(), Some(map.clone())).is_err());
    let mut oversized = map;
    oversized
        .data
        .as_mut()
        .unwrap()
        .insert(CURSOR_KEY.into(), "x".repeat(MAX_CURSOR_BYTES + 1));
    assert!(load_cursor(name, uid, "catalog-a".into(), Some(oversized)).is_err());
}

#[test]
fn persisted_cursor_survives_more_catalogs_than_an_in_memory_page() {
    // Every catalog retains its own durable hint; visiting a later page never
    // evicts or resets an earlier catalog's cursor.
    let maps: Vec<_> = (1..=CLUSTER_PAGE_SIZE * 3)
        .map(|id| {
            let uid = Uuid::from_u128(id as u128).to_string();
            let mut map = cursor_map(&uid, &format!("catalog-{id}"));
            // A deep valid History route must survive a complete discovery pass.
            let after = format!("packed/registry/history-root/{id:08}").into_bytes();
            let encoded = serde_json::json!({ "tier": "History", "after": after }).to_string();
            PackedGcCursor::decode(encoded.as_bytes()).unwrap();
            map.data
                .as_mut()
                .unwrap()
                .insert(CURSOR_KEY.into(), encoded);
            (uid.clone(), map)
        })
        .collect();
    for (index, (uid, map)) in maps.into_iter().enumerate().rev() {
        let catalog = format!("catalog-{}", index + 1);
        let expected = PackedGcCursor::decode(map.data.as_ref().unwrap()[CURSOR_KEY].as_bytes())
            .unwrap()
            .encode()
            .unwrap();
        let loaded = load_cursor(cursor_name(&uid).unwrap(), uid, catalog, Some(map)).unwrap();
        assert_eq!(loaded.cursor.encode().unwrap(), expected);
        assert_ne!(expected, PackedGcCursor::default().encode().unwrap());
    }
}

fn cluster(index: u32) -> BrewFSCluster {
    BrewFSCluster::new(
        &format!("cluster-{index}"),
        crate::crd::BrewFSClusterSpec {
            redis: Default::default(),
            rustfs: Default::default(),
            mount_config: Default::default(),
            workspace: Some(super::super::crd::WorkspaceClusterSpec {
                enabled: true,
                ..Default::default()
            }),
        },
    )
}

#[test]
fn bounded_pages_visit_each_cluster_once_before_next_page() {
    let mut queue = ClusterQueue::default();
    let mut visited = Vec::new();
    for page in 0..3 {
        queue
            .accept_page(
                Some(format!("page-{}", page + 1)),
                (page * CLUSTER_PAGE_SIZE..(page + 1) * CLUSTER_PAGE_SIZE)
                    .map(cluster)
                    .collect(),
            )
            .unwrap();
        assert!(
            queue.accept_page(None, vec![cluster(99)]).is_err(),
            "cannot discard a pending page"
        );
        while let Some(cluster) = queue.pending.pop_front() {
            visited.push(cluster.name_any());
        }
        assert_eq!(
            queue.continuation.as_deref(),
            Some(format!("page-{}", page + 1).as_str())
        );
    }
    assert_eq!(
        visited,
        (0..CLUSTER_PAGE_SIZE * 3)
            .map(|index| format!("cluster-{index}"))
            .collect::<Vec<_>>()
    );
    assert!(queue
        .accept_page(None, (0..CLUSTER_PAGE_SIZE + 1).map(cluster).collect())
        .is_err());
    assert!(queue.pending.is_empty());
}
