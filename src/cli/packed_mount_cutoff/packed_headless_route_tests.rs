//! Real original FUSE shutdown produces the only PCR used by these API cases.
//! Faults change owned backend rows with exact real CAS, never returned values.

use super::*;
use crate::workspace::packed::snapshot::SnapshotOwnedPermit;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Moment {
    AfterRoute,
    BeforeAuthentication,
}

struct Mutation {
    moment: Moment,
    check: KvCheck,
    write: KvWrite,
}

struct RouteBackend<B> {
    inner: Arc<B>,
    budget: Arc<SnapshotMountBudget>,
    workspace: WorkspaceId,
    mutation: Mutex<Option<Mutation>>,
    route_observed: AtomicBool,
    exhaust_after_route: AtomicBool,
    held_budget: Mutex<Option<SnapshotOwnedPermit>>,
    bounded_reads: AtomicUsize,
    unbounded_reads: AtomicUsize,
    mutation_attempts: AtomicUsize,
    injected: AtomicUsize,
    authentications: AtomicUsize,
    final_packet: Mutex<Vec<KvCheck>>,
}

impl<B: WorkspaceKvBackend> RouteBackend<B> {
    fn new(
        inner: Arc<B>,
        budget: Arc<SnapshotMountBudget>,
        workspace: WorkspaceId,
        mutation: Option<Mutation>,
    ) -> Self {
        Self {
            inner,
            budget,
            workspace,
            mutation: Mutex::new(mutation),
            route_observed: AtomicBool::new(false),
            exhaust_after_route: AtomicBool::new(false),
            held_budget: Mutex::new(None),
            bounded_reads: AtomicUsize::new(0),
            unbounded_reads: AtomicUsize::new(0),
            mutation_attempts: AtomicUsize::new(0),
            injected: AtomicUsize::new(0),
            authentications: AtomicUsize::new(0),
            final_packet: Mutex::new(Vec::new()),
        }
    }

    async fn inject(&self, moment: Moment) -> Result<(), WorkspaceError> {
        let mutation = {
            let mut slot = self.mutation.lock().await;
            if slot
                .as_ref()
                .is_some_and(|mutation| mutation.moment == moment)
            {
                slot.take()
            } else {
                None
            }
        };
        if let Some(mutation) = mutation {
            assert!(
                self.inner
                    .compare_and_swap(&[mutation.check], &[mutation.write])
                    .await?,
                "owned concurrent change lost its exact predecessor"
            );
            self.injected.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn observe_authentication(&self, checks: &[KvCheck]) -> Result<(), WorkspaceError> {
        assert!(checks.len() <= 32, "actual source authority count exceeded");
        let bytes: usize = checks
            .iter()
            .map(|check| check.key.len() + check.expected.as_ref().map_or(0, Vec::len))
            .sum();
        assert!(
            bytes <= 48 << 10,
            "actual source packet byte bound exceeded"
        );
        // History paging authenticates its membership epoch and individual
        // lease facts. Only the complete routed PCR packet is the final source
        // authentication boundary exercised by these faults.
        if !checks
            .iter()
            .any(|check| check.key.starts_with(b"packed/clean-release/"))
            || !checks
                .iter()
                .any(|check| check.key == format!("packed/writer/{}", self.workspace).as_bytes())
            || !checks
                .iter()
                .any(|check| check.key == workspace_key(self.workspace))
        {
            return Ok(());
        }
        self.authentications.fetch_add(1, Ordering::SeqCst);
        *self.final_packet.lock().await = checks.to_vec();
        self.inject(Moment::BeforeAuthentication).await
    }

    fn assert_readonly(&self) {
        assert_eq!(self.mutation_attempts.load(Ordering::SeqCst), 0);
        assert_eq!(self.unbounded_reads.load(Ordering::SeqCst), 0);
    }

    fn reject_mutation(&self, writes: &[KvWrite]) -> Result<(), WorkspaceError> {
        if !writes.is_empty() {
            self.mutation_attempts.fetch_add(1, Ordering::SeqCst);
            return Err(WorkspaceError::Backend(
                "readonly discovery attempted metadata writes".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl<B: WorkspaceKvBackend> WorkspaceKvBackend for RouteBackend<B> {
    fn supports_consistent_reads(&self) -> bool {
        self.inner.supports_consistent_reads()
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    async fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
        self.unbounded_reads.fetch_add(1, Ordering::SeqCst);
        Err(WorkspaceError::Backend(
            "discovery attempted an unbounded read".into(),
        ))
    }

    async fn scan_prefix(&self, _: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.unbounded_reads.fetch_add(1, Ordering::SeqCst);
        Err(WorkspaceError::Backend(
            "discovery attempted a namespace scan".into(),
        ))
    }

    async fn scan_prefix_page_with_byte_limits(
        &self,
        prefix: &[u8],
        after_key_exclusive: Option<&[u8]>,
        limits: KvReadLimits,
    ) -> Result<Vec<KvEntry>, WorkspaceError> {
        assert_eq!(
            prefix,
            format!("lease/{}/", self.workspace).as_bytes(),
            "discovery must page only the declared workspace's lease history"
        );
        assert_eq!(limits.max_records, 1);
        assert!(limits.max_key_bytes <= 1024);
        assert!(limits.max_value_bytes <= 48 << 10);
        assert!(limits.max_total_bytes <= 48 << 10);
        assert!(limits.max_response_bytes <= 64 << 10);
        assert!(limits.max_data_requests <= 32);
        self.inner
            .scan_prefix_page_with_byte_limits(prefix, after_key_exclusive, limits)
            .await
    }

    async fn get_many_consistent_with_time_bounded(
        &self,
        keys: &[Vec<u8>],
        limits: KvReadLimits,
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        self.bounded_reads.fetch_add(1, Ordering::SeqCst);
        let actual = self
            .inner
            .get_many_consistent_with_time_bounded(keys, limits)
            .await?;
        let routed = keys.contains(&workspace_key(self.workspace))
            && keys.contains(&format!("packed/writer/{}", self.workspace).into_bytes());
        if routed && !self.route_observed.swap(true, Ordering::SeqCst) {
            // Preserve the exact genuine response; only the persisted successor
            // changes before the caller can use that routing observation.
            self.inject(Moment::AfterRoute).await?;
            if self.exhaust_after_route.swap(false, Ordering::SeqCst) {
                let used = self.budget.state().used[SnapshotBudgetPool::Metadata as usize];
                let remaining = self.budget.capacity(SnapshotBudgetPool::Metadata) - used;
                let owner = self
                    .budget
                    .admit(&[(SnapshotBudgetPool::Metadata, remaining)])
                    .unwrap();
                *self.held_budget.lock().await = Some(owner);
            }
        }
        Ok(actual)
    }

    async fn compare_and_swap(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
    ) -> Result<bool, WorkspaceError> {
        self.reject_mutation(writes)?;
        self.observe_authentication(checks).await?;
        self.inner.compare_and_swap(checks, writes).await
    }

    async fn compare_and_swap_before(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        deadline: i64,
    ) -> Result<bool, WorkspaceError> {
        self.reject_mutation(writes)?;
        self.observe_authentication(checks).await?;
        self.inner
            .compare_and_swap_before(checks, writes, deadline)
            .await
    }

    async fn authenticate_checks_before_bounded(
        &self,
        checks: &[KvCheck],
        deadline: i64,
        limits: KvReadLimits,
    ) -> Result<bool, WorkspaceError> {
        self.observe_authentication(checks).await?;
        self.inner
            .authenticate_checks_before_bounded(checks, deadline, limits)
            .await
    }

    async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
        self.inner.server_time_ns().await
    }
}

#[derive(Serialize, Deserialize)]
struct WriterObservation {
    workspace_id: WorkspaceId,
    incarnation: u64,
    owner: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize)]
struct OpenObservation {
    workspace_id: WorkspaceId,
    owner_id: String,
    generation: u64,
    expires_at_ns: i64,
    state: SnapshotOpenState,
    recovery_required: bool,
}

fn envelope<T: Serialize>(value: &T) -> Vec<u8> {
    let mut raw = b"BWSKV001".to_vec();
    raw.extend(bincode::serialize(value).unwrap());
    raw
}

fn control_envelope(value: &ControlSnapshot) -> Vec<u8> {
    let mut raw = b"BWSCT002".to_vec();
    raw.extend(bincode::serialize(value).unwrap());
    raw
}

fn writer(raw: &[u8]) -> WriterObservation {
    serde_json::from_slice(raw.strip_prefix(b"PWA3\x01").unwrap()).unwrap()
}

fn writer_bytes(value: &WriterObservation) -> Vec<u8> {
    let mut raw = b"PWA3\x01".to_vec();
    raw.extend(serde_json::to_vec(value).unwrap());
    raw
}

async fn assert_rows<B: WorkspaceKvBackend>(
    backend: &B,
    keys: &[Vec<u8>],
    expected: &[Option<Vec<u8>>],
) {
    let (actual, _) = backend
        .get_many_consistent_with_time_bounded(keys, point_limits(keys.len()))
        .await
        .unwrap();
    assert_eq!(
        actual, expected,
        "discovery changed the actual owned metadata"
    );
}

async fn restore<B: WorkspaceKvBackend>(
    backend: &B,
    keys: &[Vec<u8>],
    baseline: &[Option<Vec<u8>>],
    changed: &[Option<Vec<u8>>],
) {
    let checks = keys
        .iter()
        .cloned()
        .zip(changed.iter().cloned())
        .map(|(key, expected)| KvCheck { key, expected })
        .collect::<Vec<_>>();
    let writes = keys
        .iter()
        .cloned()
        .zip(baseline.iter().cloned())
        .map(|(key, value)| match value {
            Some(value) => KvWrite::Put { key, value },
            None => KvWrite::Delete { key },
        })
        .collect::<Vec<_>>();
    assert!(
        backend.compare_and_swap(&checks, &writes).await.unwrap(),
        "exact owned fixture restoration failed"
    );
    assert_rows(backend, keys, baseline).await;
}

fn assert_fenced(result: Result<Option<PackedReleasedMountReference>, WorkspaceError>) {
    assert!(
        matches!(
            result,
            Ok(None) | Err(WorkspaceError::Busy | WorkspaceError::Fenced)
        ),
        "expected an exact source fence, got {result:?}"
    );
}

pub(super) async fn contract<B: WorkspaceKvBackend>(
    bare: Arc<B>,
    budget: Arc<SnapshotMountBudget>,
    released: PackedReleasedMountReference,
) {
    let workspace = released.guard.workspace_id;
    let keys = vec![
        b"control".to_vec(),
        workspace_key(workspace),
        format!("layer/{}", released.guard.expected_head_layer_id).into_bytes(),
        format!("packed/writer/{workspace}").into_bytes(),
        format!("open/{workspace}").into_bytes(),
        receipt_key(&released),
        lease_key(workspace, released.guard.lease_id),
        format!("packed/current/{workspace}").into_bytes(),
        b"packed/topology-generation".to_vec(),
    ];
    let (baseline, _) = bare
        .get_many_consistent_with_time_bounded(&keys, point_limits(keys.len()))
        .await
        .unwrap();
    assert_eq!(baseline.len(), keys.len());
    assert!(baseline.iter().all(Option::is_some));
    let raw = |index: usize| baseline[index].as_ref().unwrap().as_slice();
    let control = decode_control(raw(0));
    let hot: WorkspaceRecord = decode_envelope(raw(1));
    assert_eq!(control.schema_version, 1);
    assert_eq!(control.catalog_format, 2);
    assert!(control.header.is_some());
    assert_eq!(hot.workspace_id, workspace);
    assert_eq!(hot.head_epoch, released.guard.expected_head_epoch);
    assert!(hot.active_lease.is_none());
    let source_lease: SnapshotLease = decode_envelope(raw(6));
    assert_eq!(source_lease.lease_id, released.guard.lease_id);
    assert_eq!(source_lease.workspace_id, workspace);
    assert_eq!(source_lease.state, LeaseState::Released);
    assert!(writer(raw(3)).owner.is_none());

    let observed = Arc::new(RouteBackend::new(
        bare.clone(),
        budget.clone(),
        workspace,
        None,
    ));
    let store = Arc::new(
        KvWorkspaceStore::from_arc(observed.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    assert_eq!(
        store
            .inspect_original_clean_packed_mount(workspace)
            .await
            .unwrap(),
        Some(released.clone()),
        "the small CONTROL header must route actual entity epoch and genuine PCR"
    );
    observed.assert_readonly();
    assert!(observed.authentications.load(Ordering::SeqCst) > 0);
    let packet = observed.final_packet.lock().await;
    for (key, expected) in keys.iter().zip(&baseline) {
        assert!(
            packet.contains(&KvCheck {
                key: key.clone(),
                expected: expected.clone()
            }),
            "the actual final authentication omitted a routed source fact"
        );
    }
    drop(packet);
    drop(store);
    drop(observed);
    assert_rows(bare.as_ref(), &keys, &baseline).await;

    // Both routing races and locked authentication races use real persisted CAS.
    let mut changed_hot = hot.clone();
    changed_hot.updated_at_ns = changed_hot.updated_at_ns.checked_add(1).unwrap();
    let mut changed_head: LayerRecord = decode_envelope(raw(2));
    changed_head.next_sequence = changed_head.next_sequence.checked_add(1).unwrap();
    let mut changed_writer = writer(raw(3));
    changed_writer.incarnation = changed_writer.incarnation.checked_add(1).unwrap();
    let mut changed_open: OpenObservation = decode_envelope(raw(4));
    changed_open.generation = changed_open.generation.checked_add(1).unwrap();
    let races = [
        (
            "workspace route changed",
            Moment::AfterRoute,
            1,
            envelope(&changed_hot),
        ),
        (
            "workspace row changed at final authentication",
            Moment::BeforeAuthentication,
            1,
            envelope(&changed_hot),
        ),
        (
            "head changed at final authentication",
            Moment::BeforeAuthentication,
            2,
            envelope(&changed_head),
        ),
        (
            "idle PWA incarnation changed",
            Moment::BeforeAuthentication,
            3,
            writer_bytes(&changed_writer),
        ),
        (
            "open generation changed",
            Moment::BeforeAuthentication,
            4,
            envelope(&changed_open),
        ),
    ];
    for (name, moment, index, replacement) in races {
        let observed = Arc::new(RouteBackend::new(
            bare.clone(),
            budget.clone(),
            workspace,
            Some(Mutation {
                moment,
                check: KvCheck {
                    key: keys[index].clone(),
                    expected: baseline[index].clone(),
                },
                write: KvWrite::Put {
                    key: keys[index].clone(),
                    value: replacement.clone(),
                },
            }),
        ));
        let store = Arc::new(
            KvWorkspaceStore::from_arc(observed.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        assert_fenced(store.inspect_original_clean_packed_mount(workspace).await);
        assert_eq!(
            observed.injected.load(Ordering::SeqCst),
            1,
            "fault boundary was not reached: {name}"
        );
        assert!(observed.mutation.lock().await.is_none());
        observed.assert_readonly();
        let mut changed = baseline.clone();
        changed[index] = Some(replacement);
        assert_rows(bare.as_ref(), &keys, &changed).await;
        restore(bare.as_ref(), &keys, &baseline, &changed).await;
    }

    let mut foreign_writer = writer(raw(3));
    foreign_writer.workspace_id = WorkspaceId::new();
    let mut foreign_hot = hot.clone();
    foreign_hot.workspace_id = WorkspaceId::new();
    let mut foreign_open: OpenObservation = decode_envelope(raw(4));
    foreign_open.workspace_id = WorkspaceId::new();
    let mut incompatible_control = decode_control(raw(0));
    incompatible_control.schema_version =
        incompatible_control.schema_version.checked_add(1).unwrap();
    let mut foreign_source = source_lease.clone();
    foreign_source.workspace_id = WorkspaceId::new();
    let corruptions = [
        ("foreign PWA workspace", 3, writer_bytes(&foreign_writer)),
        ("foreign workspace entity", 1, envelope(&foreign_hot)),
        ("foreign open workspace", 4, envelope(&foreign_open)),
        (
            "incompatible CONTROL schema",
            0,
            control_envelope(&incompatible_control),
        ),
        (
            "foreign source lease workspace",
            6,
            envelope(&foreign_source),
        ),
        ("oversized PWA", 3, vec![0; 4097]),
        (
            "CONTROL exceeds fixed source response",
            0,
            vec![0; (48 << 10) + 1],
        ),
    ];
    for (name, index, replacement) in corruptions {
        assert!(
            bare.compare_and_swap(
                &[KvCheck {
                    key: keys[index].clone(),
                    expected: baseline[index].clone()
                }],
                &[KvWrite::Put {
                    key: keys[index].clone(),
                    value: replacement.clone()
                }]
            )
            .await
            .unwrap()
        );
        let observed = Arc::new(RouteBackend::new(
            bare.clone(),
            budget.clone(),
            workspace,
            None,
        ));
        let store = Arc::new(
            KvWorkspaceStore::from_arc(observed.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        let result = store.inspect_original_clean_packed_mount(workspace).await;
        let mut changed = baseline.clone();
        changed[index] = Some(replacement);
        if name != "CONTROL exceeds fixed source response" {
            assert_rows(bare.as_ref(), &keys, &changed).await;
        }
        // Restore by exact point CAS; never reread an oversized row unbounded.
        // This precedes the result oracle so a regression cannot strand poison.
        restore(bare.as_ref(), &keys, &baseline, &changed).await;
        assert!(
            result.is_err(),
            "malformed actual route returned facts: {name}: {result:?}"
        );
        observed.assert_readonly();
        assert_eq!(
            observed.authentications.load(Ordering::SeqCst),
            0,
            "malformed route reached source authentication: {name}"
        );
    }

    // Aliases live inside the declared workspace's actual history prefix. A
    // bounded route must reject their key/identity mismatch without requiring a
    // scan of unrelated workspaces or rewriting observations in the wrapper.
    for (name, alias) in [
        ("same-workspace source lease alias", source_lease.clone()),
        ("foreign-workspace source lease alias", foreign_source),
    ] {
        let key = lease_key(workspace, LeaseId::new());
        let value = envelope(&alias);
        assert!(
            bare.compare_and_swap(
                &[KvCheck {
                    key: key.clone(),
                    expected: None
                }],
                &[KvWrite::Put {
                    key: key.clone(),
                    value: value.clone()
                }],
            )
            .await
            .unwrap()
        );
        let observed = Arc::new(RouteBackend::new(
            bare.clone(),
            budget.clone(),
            workspace,
            None,
        ));
        let store = Arc::new(
            KvWorkspaceStore::from_arc(observed.clone())
                .with_packed_reader_pin_budget(budget.clone()),
        );
        let result = store.inspect_original_clean_packed_mount(workspace).await;
        assert_rows(bare.as_ref(), &keys, &baseline).await;
        assert_rows(
            bare.as_ref(),
            std::slice::from_ref(&key),
            &[Some(value.clone())],
        )
        .await;
        // Exact cleanup precedes the oracle so a regression cannot strand poison.
        assert!(
            bare.compare_and_swap(
                &[KvCheck {
                    key: key.clone(),
                    expected: Some(value)
                }],
                &[KvWrite::Delete { key: key.clone() }],
            )
            .await
            .unwrap()
        );
        assert_rows(bare.as_ref(), std::slice::from_ref(&key), &[None]).await;
        assert!(
            result.is_err(),
            "malformed actual history returned facts: {name}: {result:?}"
        );
        observed.assert_readonly();
        assert_eq!(
            observed.authentications.load(Ordering::SeqCst),
            0,
            "malformed history reached source authentication: {name}"
        );
    }

    let owned_before = budget.state().used;
    let remaining = budget.capacity(SnapshotBudgetPool::Metadata)
        - owned_before[SnapshotBudgetPool::Metadata as usize];
    let blocker = budget
        .admit(&[(SnapshotBudgetPool::Metadata, remaining)])
        .unwrap();
    let observed = Arc::new(RouteBackend::new(
        bare.clone(),
        budget.clone(),
        workspace,
        None,
    ));
    let store = Arc::new(
        KvWorkspaceStore::from_arc(observed.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    assert!(
        matches!(
            store.inspect_original_clean_packed_mount(workspace).await,
            Err(WorkspaceError::InvalidReadPlan(_))
        ),
        "source admission must use the already owned canonical ledger"
    );
    assert_eq!(observed.bounded_reads.load(Ordering::SeqCst), 0);
    assert_eq!(observed.authentications.load(Ordering::SeqCst), 0);
    observed.assert_readonly();
    drop(blocker);
    assert_eq!(budget.state().used, owned_before);

    let observed = Arc::new(RouteBackend::new(
        bare.clone(),
        budget.clone(),
        workspace,
        None,
    ));
    observed.exhaust_after_route.store(true, Ordering::SeqCst);
    let store = Arc::new(
        KvWorkspaceStore::from_arc(observed.clone()).with_packed_reader_pin_budget(budget.clone()),
    );
    assert!(
        store
            .inspect_original_clean_packed_mount(workspace)
            .await
            .is_err(),
        "nested source admission cannot replace an exhausted canonical ledger"
    );
    assert!(observed.held_budget.lock().await.is_some());
    assert!(observed.bounded_reads.load(Ordering::SeqCst) > 0);
    assert_eq!(observed.authentications.load(Ordering::SeqCst), 0);
    observed.assert_readonly();
    drop(observed.held_budget.lock().await.take());
    assert_eq!(budget.state().used, owned_before);
    assert_rows(bare.as_ref(), &keys, &baseline).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, Redis UUID namespace, genuine original PCR"]
async fn real_redis_original_packed_headless_route_fences_actual_successors_aliases_and_budget() {
    redis(Case::HeadlessRouteContracts).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real Linux FUSE, TiKV UUID namespace, genuine original PCR"]
async fn real_tikv_original_packed_headless_route_fences_actual_successors_aliases_and_budget() {
    tikv(Case::HeadlessRouteContracts).await;
}
