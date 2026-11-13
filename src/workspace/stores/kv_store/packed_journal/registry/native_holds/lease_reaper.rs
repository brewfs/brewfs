//! Packed native lease hold release uses durable grace and backend-clock CAS.
//! Logical lease expiry remains a separate catalog/recovery operation.

use super::*;

const REAPER_OPERATION_BYTES: u64 = 32 << 20;
const MAIN_PAGE_RECORDS: usize = 4;
const MAIN_MAX_RECORDS: usize = 128;
const MAIN_MAX_PAGES: usize = MAIN_MAX_RECORDS + 1;
const POLICY_LIMIT: usize = 1024;
const MAX_GRACE_NS: u64 = 24 * 60 * 60 * 1_000_000_000;

pub(crate) struct PackedNativeLeaseReaperOptions {
    pub lease_id: LeaseId,
    pub grace_ns: u64,
    pub max_protective_rows: u64,
    pub cancel: CancellationToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PackedNativeLeaseReaperReport {
    pub observing: bool,
    pub reaped: bool,
    pub not_before_ns: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
enum Phase {
    Observing,
    Reaped,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct Observation {
    retirement_id: Uuid,
    lease_id: LeaseId,
    workspace_id: WorkspaceId,
    holder_generation: u64,
    created_at_ns: i64,
    base_revision: BaseRevision,
    // A renewal may extend this watermark but never shorten an existing grace.
    expires_at_ns: i64,
    grace_ns: u64,
    phase: Phase,
}

impl Observation {
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        if self.retirement_id.is_nil()
            || self.lease_id.as_uuid().is_nil()
            || self.workspace_id.as_uuid().is_nil()
            || self.holder_generation == 0
            || self.created_at_ns <= 0
            || self.base_revision.layer_id.as_uuid().is_nil()
            || self.base_revision.sealed_version == 0
            || self.expires_at_ns <= self.created_at_ns
            || self.grace_ns == 0
            || self.grace_ns > MAX_GRACE_NS
        {
            return Err(journal_error("invalid packed native lease grace policy"));
        }
        self.not_before_ns()?;
        let raw = encode(self)?;
        if raw.len() > POLICY_LIMIT {
            return Err(journal_error("native lease grace policy exceeds schema"));
        }
        Ok(raw)
    }

    fn decode(raw: &[u8]) -> Result<Self, WorkspaceError> {
        let value: Self = decode_open_value(raw, POLICY_LIMIT)?;
        value.encode()?;
        Ok(value)
    }

    fn not_before_ns(&self) -> Result<i64, WorkspaceError> {
        self.expires_at_ns
            .checked_add(i64::try_from(self.grace_ns).map_err(journal_error)?)
            .ok_or_else(|| journal_error("native lease grace overflows"))
    }

    fn matches_identity(&self, lease: &SnapshotLease) -> bool {
        self.lease_id == lease.lease_id
            && self.workspace_id == lease.workspace_id
            && self.holder_generation == lease.holder_generation
            && self.created_at_ns == lease.created_at_ns
    }

    fn report(&self, reaped: bool) -> Result<PackedNativeLeaseReaperReport, WorkspaceError> {
        Ok(PackedNativeLeaseReaperReport {
            observing: !reaped,
            reaped,
            not_before_ns: self.not_before_ns()?,
        })
    }
}

fn policy_key(lease_id: LeaseId) -> Vec<u8> {
    format!("packed/native-lease-retirement/{lease_id}").into_bytes()
}

fn limits(records: usize) -> KvReadLimits {
    KvReadLimits {
        max_records: records,
        max_key_bytes: 1024,
        max_value_bytes: 16 << 10,
        max_total_bytes: 128 << 10,
        max_response_bytes: 128 << 10,
        max_data_requests: records,
    }
}

fn live(budget: &SnapshotMountBudget, cancel: &CancellationToken) -> Result<(), WorkspaceError> {
    if budget.state().closed || cancel.is_cancelled() {
        return Err(WorkspaceError::Busy);
    }
    Ok(())
}

fn proof_size(checks: &[KvCheck]) -> Result<usize, WorkspaceError> {
    let bytes = checks.iter().try_fold(0usize, |sum, check| {
        let length = check.expected.as_ref().map_or(0, Vec::len);
        if check.key.len() > 1024 || length > RECORD_LIMIT {
            return Err(journal_error(
                "native lease census point exceeds fixed tier",
            ));
        }
        sum.checked_add(check.key.len())
            .and_then(|sum| sum.checked_add(length))
            .ok_or_else(|| journal_error("native lease census aggregate overflow"))
    })?;
    if bytes > RECORD_LIMIT {
        return Err(journal_error(
            "native lease census aggregate exceeds fixed tier",
        ));
    }
    Ok(bytes)
}

struct Read {
    lease: SnapshotLease,
    workspace: WorkspaceRecord,
    head: LayerRecord,
    recovery: Option<SnapshotRecoveryRecord>,
    observation: Option<Observation>,
    checks: Vec<KvCheck>,
    now: i64,
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    async fn native_lease_clock_cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        not_before: Option<i64>,
    ) -> Result<(), WorkspaceError> {
        let packet = self
            .prepare_topology_envelope(checks.to_vec(), writes.to_vec(), None)
            .await?;
        let checks = packet.checks.as_slice();
        let writes = packet.writes.as_slice();
        proof_size(checks)?;
        let mut attempted = checks.to_vec();
        for write in writes {
            let (key, expected) = match write {
                KvWrite::Put { key, value } => (key, Some(value.clone())),
                KvWrite::Delete { key } => (key, None),
            };
            attempted
                .iter_mut()
                .find(|check| check.key == *key)
                .ok_or(WorkspaceError::Fenced)?
                .expected = expected;
        }
        proof_size(&attempted)?;
        self.history_clock_cas(checks, writes, not_before).await
    }

    async fn read_packed_native_lease_reaper(
        &self,
        lease_id: LeaseId,
    ) -> Result<Read, WorkspaceError> {
        // Routing values only select exact keys. The final bounded batch must
        // contain the same bytes; no routed snapshot grants retirement.
        let index_key = hot_lease_index_key(lease_id);
        let (index_route, _) = self
            .backend
            .get_many_consistent_with_time_bounded(std::slice::from_ref(&index_key), limits(1))
            .await?;
        if index_route.len() != 1 {
            return Err(journal_error("short native lease index routing read"));
        }
        let workspace_id: WorkspaceId =
            decode_open_value(index_route[0].as_deref().ok_or(WorkspaceError::Fenced)?, 64)?;
        let lease_key = hot_lease_key(workspace_id, lease_id);
        let (routed, _) = self
            .backend
            .get_many_consistent_with_time_bounded(std::slice::from_ref(&lease_key), limits(1))
            .await?;
        if routed.len() != 1 {
            return Err(journal_error("short native lease routing read"));
        }
        let lease: SnapshotLease =
            decode_open_value(routed[0].as_deref().ok_or(WorkspaceError::Fenced)?, 4096)?;
        if lease.lease_id != lease_id || lease.workspace_id != workspace_id {
            return Err(WorkspaceError::Fenced);
        }
        let workspace_key = hot_workspace_key(lease.workspace_id);
        let (workspace_route, _) = self
            .backend
            .get_many_consistent_with_time_bounded(std::slice::from_ref(&workspace_key), limits(1))
            .await?;
        if workspace_route.len() != 1 {
            return Err(journal_error("short native lease workspace routing read"));
        }
        let workspace: WorkspaceRecord = decode_open_value(
            workspace_route[0]
                .as_deref()
                .ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        if workspace.workspace_id != lease.workspace_id {
            return Err(WorkspaceError::Fenced);
        }
        let hold_key = [HOLD_PREFIX, format!("lease/{lease_id}").as_bytes()].concat();
        let keys = vec![
            HOLD_FEATURE.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            lease_key,
            hold_key,
            policy_key(lease_id),
            workspace_key,
            hot_layer_key(workspace.head_layer_id),
            open_snapshot_recovery_key(lease.workspace_id),
            index_key,
        ];
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(journal_error("short native lease reaper basis"));
        }
        if values[3] != routed[0] || values[6] != workspace_route[0] || values[9] != index_route[0]
        {
            return Err(WorkspaceError::Busy);
        }
        require_active_native_hold_retirement_gate(&values[0])?;
        next_packed_root_generation(&values[1])?;
        layer_inventory_generation(&values[2])?;
        if lease.holder_generation == 0
            || lease.created_at_ns <= 0
            || lease.expires_at_ns <= lease.created_at_ns
        {
            return Err(WorkspaceError::Fenced);
        }
        if lease.state == LeaseState::Released && workspace.active_lease == Some(lease_id) {
            return Err(WorkspaceError::Fenced);
        }
        let actual = values[4].as_deref().map(NativeHold::decode).transpose()?;
        if actual != NativeHold::lease(&lease) {
            return Err(WorkspaceError::Fenced);
        }
        let observation = values[5].as_deref().map(Observation::decode).transpose()?;
        if observation
            .as_ref()
            .is_some_and(|observation| !observation.matches_identity(&lease))
        {
            return Err(WorkspaceError::Fenced);
        }
        let head: LayerRecord = decode_open_value(
            values[7].as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        if head.layer_id != workspace.head_layer_id
            || head.schema_version != WORKSPACE_SCHEMA_VERSION
            || head.depth == 0
            || head.depth > LAYER_CHAIN_HARD_LIMIT
        {
            return Err(WorkspaceError::Fenced);
        }
        let recovery: Option<SnapshotRecoveryRecord> = values[8]
            .as_deref()
            .map(|raw| decode_open_value(raw, OPEN_RECOVERY_MAX_BYTES))
            .transpose()?;
        if recovery
            .as_ref()
            .is_some_and(|recovery| recovery.workspace_id != workspace.workspace_id)
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(Read {
            lease,
            workspace,
            head,
            recovery,
            observation,
            checks: keys
                .into_iter()
                .zip(values)
                .map(|(key, expected)| KvCheck { key, expected })
                .collect(),
            now,
        })
    }

    async fn packed_native_lease_no_protective_journal(
        &self,
        read: &mut Read,
        options: &PackedNativeLeaseReaperOptions,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<Option<SnapshotOwnedPermit>, WorkspaceError> {
        if !matches!(
            read.workspace.state,
            WorkspaceState::Active | WorkspaceState::Deleting
        ) || read.head.state == LayerState::Sealing
            || read
                .recovery
                .as_ref()
                .is_some_and(|record| record.incomplete)
        {
            return Err(WorkspaceError::Busy);
        }
        if read.workspace.head_epoch == 0
            || read.head.depth < 2
            || read.head.parent_layer_id.is_none()
            || read.head.sealed_version.is_some()
            || read.head.delta_digest.is_some()
            || read.head.root_hash.is_some()
            || match read.workspace.state {
                WorkspaceState::Active => {
                    read.head.state != LayerState::Writable
                        || read.head.owner_workspace_id != Some(read.workspace.workspace_id)
                }
                WorkspaceState::Deleting => {
                    read.head.state != LayerState::Deleting
                        || read.head.owner_workspace_id.is_some()
                }
                _ => true,
            }
        {
            return Err(WorkspaceError::Fenced);
        }
        if !self
            .hold_revision_contains(&read.lease.base_revision, &read.lease.base_revision, &[])
            .await?
        {
            return Err(WorkspaceError::Fenced);
        }
        let prefix = [HOLD_PREFIX, b"journal/"].concat();
        let mut after = None;
        let mut visited = 0u64;
        loop {
            live(budget, &options.cancel)?;
            proof_size(&read.checks)?;
            if !self.backend.compare_and_swap(&read.checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    &prefix,
                    after.as_deref(),
                    KvReadLimits {
                        max_records: 32,
                        max_value_bytes: HOLD_LIMIT,
                        max_total_bytes: 32 << 10,
                        max_response_bytes: 64 << 10,
                        max_data_requests: 1024,
                        ..limits(32)
                    },
                )
                .await?;
            if !self.backend.compare_and_swap(&read.checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            live(budget, &options.cancel)?;
            if page.is_empty() {
                break;
            }
            for entry in page {
                live(budget, &options.cancel)?;
                if !entry.key.starts_with(&prefix)
                    || after.as_ref().is_some_and(|last| entry.key <= *last)
                {
                    return Err(WorkspaceError::Fenced);
                }
                visited = increment(visited)?;
                if visited > options.max_protective_rows {
                    return Err(WorkspaceError::Busy);
                }
                let hold = NativeHold::decode(&entry.value)?;
                if entry.key != hold.key() {
                    return Err(WorkspaceError::Fenced);
                }
                match hold {
                    NativeHold::Journal { workspace, .. }
                        if workspace == read.lease.workspace_id =>
                    {
                        return Err(WorkspaceError::Busy);
                    }
                    NativeHold::Journal { .. } => {}
                    _ => return Err(WorkspaceError::Fenced),
                }
                after = Some(entry.key);
            }
        }
        let (_, journal_checks, owner) = self.scan_packed_journal_layer_roots().await?;
        let absent_feature = match journal_checks
            .iter()
            .find(|check| check.key.as_slice() == JOURNAL_FEATURE_KEY)
            .ok_or(WorkspaceError::Fenced)?
            .expected
            .as_deref()
        {
            None => true,
            Some(b"PPJ3") => false,
            _ => return Err(WorkspaceError::Fenced),
        };
        for check in &journal_checks {
            if check.key.starts_with(JOURNAL_PREFIX) {
                let record = PackedJournalRecord::decode(
                    check.expected.as_deref().ok_or(WorkspaceError::Fenced)?,
                )?;
                if record.guard.workspace_id == read.lease.workspace_id
                    || record.guard.lease_id == read.lease.lease_id
                {
                    return Err(WorkspaceError::Busy);
                }
            }
        }
        merge(&mut read.checks, journal_checks)?;
        let mut proof_bytes = proof_size(&read.checks)?;
        live(budget, &options.cancel)?;
        if !self.backend.compare_and_swap(&read.checks, &[]).await? {
            return Err(WorkspaceError::Busy);
        }
        // The validated PPJ3 scanner returns a complete 16 MiB owner. Reuse
        // it through MAIN/final CAS/unknown confirmation. A missing sentinel
        // returns only 1 MiB: admit the complete owner before dropping that
        // probe, so ownership never disappears while checks are retained.
        let scanner_owner = owner.ok_or(WorkspaceError::Fenced)?;
        let main_owner = if absent_feature {
            let admitted = budget
                .admit(&[(SnapshotBudgetPool::Metadata, 16 << 20)])
                .map_err(journal_budget_error)?;
            drop(scanner_owner);
            admitted
        } else {
            scanner_owner
        };
        if absent_feature {
            // Canonical journal birth creates both sentinels. A missing pair
            // cannot conceal a leftover ACTIVE row after corruption.
            live(budget, &options.cancel)?;
            proof_size(&read.checks)?;
            if !self.backend.compare_and_swap(&read.checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            let actual = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    ACTIVE_PREFIX,
                    None,
                    KvReadLimits {
                        max_records: 1,
                        max_value_bytes: RECORD_LIMIT,
                        max_total_bytes: 64 << 10,
                        max_data_requests: 1024,
                        ..limits(1)
                    },
                )
                .await?;
            live(budget, &options.cancel)?;
            proof_size(&read.checks)?;
            if !self.backend.compare_and_swap(&read.checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            if !actual.is_empty() {
                return Err(WorkspaceError::Fenced);
            }
        }
        // ACTIVE alone is insufficient even with PPJ3/count=0: a Building
        // MAIN can remain after ACTIVE loss. Census every sentinel state.
        let mut after: Option<Vec<u8>> = None;
        let mut main_visited = 0usize;
        let mut complete = false;
        for _ in 0..MAIN_MAX_PAGES {
            live(budget, &options.cancel)?;
            proof_size(&read.checks)?;
            if !self.backend.compare_and_swap(&read.checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    JOURNAL_PREFIX,
                    after.as_deref(),
                    KvReadLimits {
                        max_records: MAIN_PAGE_RECORDS,
                        max_key_bytes: 256,
                        max_value_bytes: RECORD_LIMIT,
                        max_total_bytes: 256 << 10,
                        max_response_bytes: 256 << 10,
                        max_data_requests: 64,
                    },
                )
                .await?;
            live(budget, &options.cancel)?;
            if page.len() > MAIN_PAGE_RECORDS {
                return Err(WorkspaceError::Fenced);
            }
            let empty = page.is_empty();
            for entry in page {
                live(budget, &options.cancel)?;
                main_visited += 1;
                visited = increment(visited)?;
                if main_visited > MAIN_MAX_RECORDS || visited > options.max_protective_rows {
                    return Err(WorkspaceError::Busy);
                }
                if !entry.key.starts_with(JOURNAL_PREFIX)
                    || entry.key.len() > 256
                    || entry.value.len() > RECORD_LIMIT
                    || after.as_ref().is_some_and(|cursor| entry.key <= *cursor)
                {
                    return Err(WorkspaceError::Fenced);
                }
                if let Some(old) = read.checks.iter().find(|check| check.key == entry.key) {
                    if old.expected.as_deref() != Some(entry.value.as_slice()) {
                        return Err(WorkspaceError::Busy);
                    }
                } else {
                    proof_bytes = proof_bytes
                        .checked_add(entry.key.len())
                        .and_then(|total| total.checked_add(entry.value.len()))
                        .filter(|total| *total <= RECORD_LIMIT)
                        .ok_or_else(|| {
                            journal_error("native lease census aggregate exceeds fixed tier")
                        })?;
                }
                let record = PackedJournalRecord::decode(&entry.value)?;
                if entry.key != journal_key(record.journal_id) || record.encode()? != entry.value {
                    return Err(WorkspaceError::Fenced);
                }
                // Atomic journal birth creates the sentinels with MAIN. Any
                // retained MAIN without them is corrupt, even in this domain.
                if absent_feature {
                    return Err(WorkspaceError::Fenced);
                }
                if !record.phase.terminal()
                    && (record.guard.workspace_id == read.lease.workspace_id
                        || record.guard.lease_id == read.lease.lease_id)
                {
                    return Err(WorkspaceError::Busy);
                }
                if !record.phase.terminal()
                    && !read.checks.iter().any(|check| {
                        check.key == active_key(record.journal_id)
                            && check.expected.as_deref() == Some(entry.value.as_slice())
                    })
                {
                    return Err(WorkspaceError::Fenced);
                }
                after = Some(entry.key.clone());
                merge(
                    &mut read.checks,
                    [KvCheck {
                        key: entry.key,
                        expected: Some(entry.value),
                    }],
                )?;
            }
            proof_bytes = proof_size(&read.checks)?;
            if !self.backend.compare_and_swap(&read.checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            live(budget, &options.cancel)?;
            if empty {
                complete = true;
                break;
            }
        }
        if !complete {
            return Err(WorkspaceError::Busy);
        }
        live(budget, &options.cancel)?;
        Ok(Some(main_owner))
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Exact target reaping is bounded; callers never supply a wall-clock proof.
    /// Dropping the waiter leaves this owned driver and its admission alive.
    pub(crate) async fn reap_packed_native_lease(
        self: &Arc<Self>,
        budget: Arc<SnapshotMountBudget>,
        options: super::super::PackedNativeLeaseReaperOptions,
    ) -> Result<super::super::PackedNativeLeaseReaperReport, WorkspaceError> {
        self.configure_packed_reader_pin_budget(budget.clone())?;
        live(&budget, &options.cancel)?;
        if options.lease_id.as_uuid().is_nil()
            || options.grace_ns == 0
            || options.grace_ns > MAX_GRACE_NS
            || options.max_protective_rows == 0
            || options.max_protective_rows > MAX_OBJECTS
        {
            return Err(WorkspaceError::Busy);
        }
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, REAPER_OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = store.reap_packed_native_lease_owned(&budget, options).await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| journal_error("packed native lease reaper stopped"))?
    }

    async fn reap_packed_native_lease_owned(
        self: &Arc<Self>,
        budget: &Arc<SnapshotMountBudget>,
        options: PackedNativeLeaseReaperOptions,
    ) -> Result<PackedNativeLeaseReaperReport, WorkspaceError> {
        live(budget, &options.cancel)?;
        let mut read = self
            .read_packed_native_lease_reaper(options.lease_id)
            .await?;
        if read
            .observation
            .as_ref()
            .is_some_and(|observation| observation.grace_ns != options.grace_ns)
        {
            return Err(WorkspaceError::Fenced);
        }
        if read.lease.state == LeaseState::Released {
            if let Some(observation) = &read.observation
                && observation.phase == Phase::Reaped
            {
                if observation.base_revision != read.lease.base_revision
                    || observation.expires_at_ns < read.lease.expires_at_ns
                {
                    return Err(WorkspaceError::Fenced);
                }
                self.native_lease_clock_cas(&read.checks, &[], Some(observation.not_before_ns()?))
                    .await?;
                return observation.report(true);
            }
            return Ok(PackedNativeLeaseReaperReport {
                observing: false,
                reaped: false,
                not_before_ns: 0,
            });
        }
        if !matches!(read.lease.state, LeaseState::Active | LeaseState::Expired) {
            return Err(WorkspaceError::Busy);
        }
        if read
            .observation
            .as_ref()
            .is_some_and(|observation| observation.phase != Phase::Observing)
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut observation = read.observation.clone().unwrap_or_else(|| Observation {
            retirement_id: Uuid::new_v4(),
            lease_id: read.lease.lease_id,
            workspace_id: read.lease.workspace_id,
            holder_generation: read.lease.holder_generation,
            created_at_ns: read.lease.created_at_ns,
            base_revision: read.lease.base_revision.clone(),
            expires_at_ns: read.lease.expires_at_ns,
            grace_ns: options.grace_ns,
            phase: Phase::Observing,
        });
        observation.expires_at_ns = observation.expires_at_ns.max(read.lease.expires_at_ns);
        observation.base_revision = read.lease.base_revision.clone();
        if read.observation.as_ref() != Some(&observation) {
            let generation = read
                .checks
                .iter()
                .find(|check| check.key.as_slice() == PACKED_ROOT_GENERATION_KEY)
                .ok_or(WorkspaceError::Fenced)?;
            let writes = vec![
                KvWrite::Put {
                    key: policy_key(options.lease_id),
                    value: observation.encode()?,
                },
                put(
                    PACKED_ROOT_GENERATION_KEY.to_vec(),
                    &next_packed_root_generation(&generation.expected)?,
                )?,
            ];
            live(budget, &options.cancel)?;
            self.native_lease_clock_cas(&read.checks, &writes, Some(read.now))
                .await?;
            return observation.report(false);
        }
        let lower = observation.not_before_ns()?;
        if read.now < lower {
            live(budget, &options.cancel)?;
            self.native_lease_clock_cas(&read.checks, &[], Some(read.now))
                .await?;
            return observation.report(false);
        }
        let _journals = self
            .packed_native_lease_no_protective_journal(&mut read, &options, budget)
            .await?;
        let mut successor = read.lease;
        successor.state = LeaseState::Released;
        successor.updated_at_ns = read.now;
        observation.phase = Phase::Reaped;
        let mut writes = vec![
            put(
                hot_lease_key(successor.workspace_id, successor.lease_id),
                &successor,
            )?,
            KvWrite::Put {
                key: policy_key(successor.lease_id),
                value: observation.encode()?,
            },
        ];
        if read.workspace.active_lease == Some(successor.lease_id) {
            let mut workspace = read.workspace.clone();
            workspace.active_lease = None;
            workspace.updated_at_ns = read.now;
            writes.push(put(hot_workspace_key(workspace.workspace_id), &workspace)?);
        }
        let _native = self
            .prepare_native_owner_cas(&mut read.checks, &mut writes)
            .await?;
        live(budget, &options.cancel)?;
        self.native_lease_clock_cas(&read.checks, &writes, Some(lower))
            .await?;
        observation.report(true)
    }
}

#[cfg(test)]
#[path = "lease_reaper_tests.rs"]
mod tests;
