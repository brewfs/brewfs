//! Historical physical root retirement uses actual owner censuses and clock CAS.
//! A live workspace keeps its initial lineage anchor. Deleting workspaces may
//! retire that final physical root after all current bindings and owners drain.

use super::collector::{GATE_KEY, require_active_retirement_gate};
use super::native_holds::{HOLD_FEATURE, require_active_native_hold_retirement_gate};
use super::*;

const HISTORY_OPERATION_BYTES: u64 = 32 << 20;
const MAX_HISTORY_GRACE_NS: u64 = 24 * 60 * 60 * 1_000_000_000;
const OBSERVATION_LIMIT: usize = 512;

pub(crate) struct PackedHistoryRetirementOptions {
    pub incarnation: Uuid,
    pub grace_ns: u64,
    pub max_native_holds: u64,
    pub max_current_bindings: u64,
    /// Combined member-release and queued-DELETE work for this invocation.
    pub max_work: u64,
    pub cancel: CancellationToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PackedHistoryRetirementReport {
    pub observing: bool,
    pub retired: bool,
    pub not_before_ns: i64,
    pub released_members: u64,
    pub deleted_objects: u64,
    pub quarantined_objects: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
enum ObservationPhase {
    Observing,
    Committed,
    Finished,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct Observation {
    retirement_id: Uuid,
    incarnation: Uuid,
    binding_digest: [u8; 32],
    root_revision: u64,
    // Generation after the observation CAS, which does not advance it.
    successor_generation: u64,
    layer_generation: u64,
    registry_gate_digest: [u8; 32],
    native_gate_digest: [u8; 32],
    observed_at_ns: i64,
    grace_ns: u64,
    phase: ObservationPhase,
}
impl Observation {
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        if self.retirement_id.is_nil()
            || self.incarnation.is_nil()
            || self.binding_digest == [0; 32]
            || self.root_revision == 0
            || self.successor_generation == 0
            || self.registry_gate_digest == [0; 32]
            || self.native_gate_digest == [0; 32]
            || self.observed_at_ns <= 0
            || self.grace_ns == 0
            || self.grace_ns > MAX_HISTORY_GRACE_NS
        {
            return Err(journal_error("invalid historical retirement observation"));
        }
        self.not_before_ns()?;
        let raw = encode(self)?;
        if raw.len() > OBSERVATION_LIMIT {
            return Err(journal_error(
                "historical retirement observation exceeds schema",
            ));
        }
        Ok(raw)
    }
    fn decode(raw: &[u8]) -> Result<Self, WorkspaceError> {
        let value: Self = decode_open_value(raw, OBSERVATION_LIMIT)?;
        value.encode()?;
        Ok(value)
    }
    fn not_before_ns(&self) -> Result<i64, WorkspaceError> {
        self.observed_at_ns
            .checked_add(i64::try_from(self.grace_ns).map_err(journal_error)?)
            .ok_or_else(|| journal_error("historical retirement grace overflows"))
    }
    fn matches(
        &self,
        root: &RootRow,
        binding: &PackedLowerBindingRecord,
    ) -> Result<bool, WorkspaceError> {
        let digest: [u8; 32] = Sha256::digest(binding.encode()?).into();
        Ok(self.incarnation == root.incarnation && self.binding_digest == digest)
    }
}

fn observation_key(incarnation: Uuid) -> Vec<u8> {
    format!(
        "packed/registry/history-retirement/{}",
        incarnation.simple()
    )
    .into_bytes()
}
fn delete_queue_key(incarnation: Uuid, ordinal: u64) -> Vec<u8> {
    format!(
        "packed/registry/history-delete-queue/{}/{ordinal:016x}",
        incarnation.simple()
    )
    .into_bytes()
}
fn history_limits() -> KvReadLimits {
    KvReadLimits {
        max_records: 1024,
        max_key_bytes: 1024,
        max_value_bytes: RECORD_LIMIT,
        max_total_bytes: 8 << 20,
        max_response_bytes: 128 << 10,
        max_data_requests: 1024,
    }
}
fn history_pages() -> KvReadLimits {
    KvReadLimits {
        max_records: 32,
        max_value_bytes: REGISTRY_RECORD_LIMIT,
        max_total_bytes: 512 << 10,
        max_response_bytes: 512 << 10,
        ..history_limits()
    }
}
fn history_live(
    budget: &SnapshotMountBudget,
    cancel: &CancellationToken,
) -> Result<(), WorkspaceError> {
    if budget.state().closed || cancel.is_cancelled() {
        return Err(WorkspaceError::Busy);
    }
    Ok(())
}
fn history_merge(checks: &mut Vec<KvCheck>, added: &[KvCheck]) -> Result<(), WorkspaceError> {
    for check in added {
        if let Some(old) = checks.iter().find(|old| old.key == check.key) {
            if old.expected != check.expected {
                return Err(WorkspaceError::Busy);
            }
        } else {
            checks.push(check.clone());
        }
    }
    Ok(())
}

fn history_gate_digest(checks: &[KvCheck], key: &[u8]) -> Result<[u8; 32], WorkspaceError> {
    let mut found = checks.iter().filter(|check| check.key.as_slice() == key);
    let raw = found
        .next()
        .and_then(|check| check.expected.as_deref())
        .ok_or(WorkspaceError::Fenced)?;
    if found.next().is_some() {
        return Err(WorkspaceError::Fenced);
    }
    Ok(Sha256::digest(raw).into())
}

struct HistoryRead {
    checks: Vec<KvCheck>,
    root: RootRow,
    binding: PackedLowerBindingRecord,
    observation: Option<Observation>,
    now: i64,
    current: Option<Vec<u8>>,
    generation: Option<Vec<u8>>,
    layer_generation: u64,
}

#[derive(Clone, Copy)]
pub(super) struct HistoryDeleteContext {
    pub(super) incarnation: Uuid,
    pub(super) ordinal: u64,
    pub(super) lower: i64,
}

fn history_require_committed(read: &HistoryRead, lower: i64) -> Result<(), WorkspaceError> {
    if !matches!(read.root.state, RootState::Retiring | RootState::Retired)
        || read
            .observation
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?
            .not_before_ns()?
            != lower
    {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    async fn history_member_source_checks(
        &self,
        read: &mut HistoryRead,
        member: &MemberRow,
        reference: &SnapshotObjectRef,
    ) -> Result<(), WorkspaceError> {
        let actual = read
            .checks
            .iter()
            .find(|check| check.key == journal_key(read.root.journal_id))
            .ok_or(WorkspaceError::Fenced)?;
        if member.adopted {
            if actual.expected.is_some() || !member.put_id.is_nil() || member.dispatched {
                return Err(WorkspaceError::Fenced);
            }
        } else {
            let journal = PackedJournalRecord::decode(
                actual.expected.as_deref().ok_or(WorkspaceError::Fenced)?,
            )?;
            if journal.phase != PackedJournalPhase::Committed
                || journal.journal_id != read.root.journal_id
                || journal.source.staging_id != read.root.incarnation
                || journal.commit_target.as_ref() != Some(&read.binding)
                || !member.dispatched
                || member.put_id.is_nil()
                || member.ordinal >= journal.object_count
            {
                return Err(WorkspaceError::Fenced);
            }
            let key = object_key(journal.journal_id, member.ordinal);
            let (actual, _) = self
                .backend
                .get_many_consistent_with_time_bounded(std::slice::from_ref(&key), history_limits())
                .await?;
            if actual.len() != 1 {
                return Err(WorkspaceError::Fenced);
            }
            let row =
                PackedJournalObject::decode(actual[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
            if !row.uploaded || row.reference != *reference || row.ordinal != member.ordinal {
                return Err(WorkspaceError::Fenced);
            }
            history_merge(
                &mut read.checks,
                &[KvCheck {
                    key,
                    expected: actual[0].clone(),
                }],
            )?;
        }
        Ok(())
    }

    // Each actual DELETE guard transition consumes a fresh historical proof.
    // Neither a caller's old scan nor a newly activated gate can replace it.
    pub(super) async fn history_retirement_delete_checks(
        &self,
        context: HistoryDeleteContext,
        reference: &SnapshotObjectRef,
    ) -> Result<Vec<KvCheck>, WorkspaceError> {
        let mut read = self.history_retirement_read(context.incarnation).await?;
        history_require_committed(&read, context.lower)?;
        let keys = [
            delete_queue_key(context.incarnation, context.ordinal),
            registry_member_key(reference, context.incarnation),
        ];
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, history_limits())
            .await?;
        if values.len() != keys.len()
            || values[0].as_deref()
                != Some(reference.encode_value().map_err(journal_error)?.as_slice())
        {
            return Err(WorkspaceError::Fenced);
        }
        let member = MemberRow::decode(values[1].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if member.retained
            || member.pending_put
            || member.reference != *reference
            || member.incarnation != context.incarnation
            || member.ordinal != context.ordinal
            || member.journal_id != read.root.journal_id
        {
            return Err(WorkspaceError::Fenced);
        }
        history_merge(
            &mut read.checks,
            &keys
                .into_iter()
                .zip(values)
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>(),
        )?;
        self.history_member_source_checks(&mut read, &member, reference)
            .await?;
        Ok(read.checks)
    }

    async fn history_retirement_read(
        &self,
        incarnation: Uuid,
    ) -> Result<HistoryRead, WorkspaceError> {
        let (route, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                &[registry_root_key(incarnation)],
                history_limits(),
            )
            .await?;
        if route.len() != 1 {
            return Err(journal_error("short historical root route"));
        }
        let root = RootRow::decode(route[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        let binding = root.binding.clone().ok_or(WorkspaceError::Fenced)?;
        if root.incarnation != incarnation
            || binding.binding.binding_version == 0
            || root.pending_puts != 0
            || !matches!(
                root.state,
                RootState::BindingHistory | RootState::Retiring | RootState::Retired
            )
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut keys = vec![
            GATE_KEY.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            registry_root_key(incarnation),
            registry_history_root_key(&binding),
            packed_history_key(binding.workspace_id, binding.binding.binding_version),
            packed_current_key(binding.workspace_id),
            packed_claim_key(binding.workspace_id),
            observation_key(incarnation),
            journal_key(root.journal_id),
            active_key(root.journal_id),
            HOLD_FEATURE.to_vec(),
        ];
        let deleting_workspace_index = if binding.binding.binding_version == 1 {
            let index = keys.len();
            keys.push(hot_workspace_key(binding.workspace_id));
            Some(index)
        } else {
            None
        };
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, history_limits())
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Busy);
        }
        if let Some(index) = deleting_workspace_index {
            // History 1 is an integrity sentinel for every live view. Its
            // physical graph can disappear only after durable deletion, and a
            // newer current must finish retirement before the sentinel goes.
            // The workspace row is also consumed by every subsequent CAS,
            // including grace observation, DELETE guards and resumed pickup.
            let workspace: WorkspaceRecord = decode_open_value(
                values[index].as_deref().ok_or(WorkspaceError::Fenced)?,
                OPEN_RECORD_MAX_BYTES,
            )?;
            if workspace.workspace_id != binding.workspace_id
                || workspace.state != WorkspaceState::Deleting
                || values[6]
                    .as_deref()
                    .map(PackedLowerBindingRecord::decode)
                    .transpose()?
                    .is_some_and(|current| current != binding)
            {
                return Err(WorkspaceError::Fenced);
            }
        }
        if values[3] != route[0] || values[4] != values[3] {
            return Err(WorkspaceError::Busy);
        }
        require_active_retirement_gate(&values[0])?;
        require_active_native_hold_retirement_gate(&values[11])?;
        if values[10].is_some() {
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[1])?;
        let layer_generation = layer_inventory_generation(&values[2])?;
        let observation = values[8].as_deref().map(Observation::decode).transpose()?;
        if let Some(observation) = &observation
            && (!observation.matches(&root, &binding)? || observation.root_revision > root.revision)
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut checks = keys
            .into_iter()
            .zip(values.iter().cloned())
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        // Validate the complete current/claim/history pair, including a newer
        // current's own history. A target current can never survive retirement.
        if let Some(raw) = &values[6] {
            let current = PackedLowerBindingRecord::decode(raw)?;
            if current.workspace_id != binding.workspace_id
                || (root.state != RootState::BindingHistory && current == binding)
            {
                return Err(WorkspaceError::Fenced);
            }
            let current_history_key =
                packed_history_key(current.workspace_id, current.binding.binding_version);
            let current_history =
                if current.binding.binding_version == binding.binding.binding_version {
                    values[5].clone()
                } else {
                    let (actual, _) = self
                        .backend
                        .get_many_consistent_with_time_bounded(
                            std::slice::from_ref(&current_history_key),
                            history_limits(),
                        )
                        .await?;
                    if actual.len() != 1 {
                        return Err(WorkspaceError::Fenced);
                    }
                    history_merge(
                        &mut checks,
                        &[KvCheck {
                            key: current_history_key,
                            expected: actual[0].clone(),
                        }],
                    )?;
                    actual[0].clone()
                };
            if decode_packed_pair(
                binding.workspace_id,
                &values[6],
                &values[7],
                &current_history,
            )?
            .as_ref()
                != Some(&current)
            {
                return Err(WorkspaceError::Fenced);
            }
            if root.state != RootState::BindingHistory {
                let base_contains = self
                    .hold_revision_contains(&current.base_revision, &binding.base_revision, &[])
                    .await?;
                let head_contains = self
                    .hold_chain_contains(current.head_layer_id, &binding.base_revision, &[])
                    .await?;
                if base_contains
                    || head_contains
                    || !self
                        .hold_chain_contains(current.head_layer_id, &current.base_revision, &[])
                        .await?
                {
                    return Err(WorkspaceError::Fenced);
                }
            }
        } else if values[7].is_some() {
            return Err(WorkspaceError::Fenced);
        }
        if root.state == RootState::BindingHistory {
            if values[5].as_deref() != Some(binding.encode()?.as_slice()) {
                return Err(WorkspaceError::Fenced);
            }
            if root.members == 0
                || observation
                    .as_ref()
                    .is_some_and(|observation| observation.phase != ObservationPhase::Observing)
            {
                return Err(WorkspaceError::Fenced);
            }
            if let Some(raw) = &values[9] {
                let journal = PackedJournalRecord::decode(raw)?;
                if journal.phase != PackedJournalPhase::Committed
                    || journal.journal_id != root.journal_id
                    || journal.source.staging_id != incarnation
                    || journal.commit_target.as_ref() != Some(&binding)
                    || journal.object_count != root.members
                {
                    return Err(WorkspaceError::Fenced);
                }
            }
        } else {
            let observation = observation.as_ref().ok_or(WorkspaceError::Fenced)?;
            if values[5].is_some()
                || !observation.matches(&root, &binding)?
                || observation.registry_gate_digest != history_gate_digest(&checks, GATE_KEY)?
                || observation.native_gate_digest != history_gate_digest(&checks, HOLD_FEATURE)?
                || root.revision <= observation.root_revision
                || (root.state == RootState::Retiring
                    && observation.phase != ObservationPhase::Committed)
                || (root.state == RootState::Retired
                    && observation.phase != ObservationPhase::Finished)
            {
                return Err(WorkspaceError::Fenced);
            }
        }
        Ok(HistoryRead {
            checks,
            root,
            binding,
            observation,
            now,
            current: values[6].clone(),
            generation: values[1].clone(),
            layer_generation,
        })
    }

    /// Uncertain write replies authorize only exact successor/clock confirmation.
    pub(super) async fn history_clock_cas(
        &self,
        checks: &[KvCheck],
        writes: &[KvWrite],
        not_before: Option<i64>,
    ) -> Result<(), WorkspaceError> {
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
        let limits = history_limits();
        let bytes = attempted.iter().try_fold(0usize, |sum, check| {
            let length = check.expected.as_ref().map_or(0, Vec::len);
            if check.key.len() > limits.max_key_bytes || length > limits.max_value_bytes {
                return Err(WorkspaceError::Fenced);
            }
            sum.checked_add(check.key.len())
                .and_then(|sum| sum.checked_add(length))
                .ok_or(WorkspaceError::Fenced)
        })?;
        if attempted.len() > limits.max_records || bytes > limits.max_total_bytes {
            return Err(WorkspaceError::Busy);
        }
        match self
            .backend
            .compare_and_swap_in_time_window(checks, writes, not_before, None)
            .await
        {
            Ok(true) => Ok(()),
            Ok(false) => Err(WorkspaceError::Busy),
            Err(error @ WorkspaceError::Backend(_)) => {
                let confirmed = async {
                    // At most two 48 KiB values fit the fixed 128 KiB raw
                    // response tier on Redis as well as TiKV. The final full
                    // no-op CAS authenticates all batches at one clock window.
                    for batch in attempted.chunks(2) {
                        let keys = batch
                            .iter()
                            .map(|check| check.key.clone())
                            .collect::<Vec<_>>();
                        let (actual, now) = self
                            .backend
                            .get_many_consistent_with_time_bounded(&keys, limits)
                            .await?;
                        if actual.len() != batch.len()
                            || now <= 0
                            || not_before.is_some_and(|lower| now < lower)
                            || actual
                                .iter()
                                .zip(batch)
                                .any(|(actual, check)| *actual != check.expected)
                        {
                            return Ok(false);
                        }
                    }
                    self.backend
                        .compare_and_swap_in_time_window(&attempted, &[], not_before, None)
                        .await
                }
                .await;
                if matches!(confirmed, Ok(true)) {
                    Ok(())
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }
}

impl<B: WorkspaceKvBackend + 'static> KvWorkspaceStore<B> {
    /// The complete driver remains owned after its waiting caller is dropped.
    pub(crate) async fn retire_packed_binding_history<O: ObjectBackend + Clone + 'static>(
        self: &Arc<Self>,
        client: ObjectClient<O>,
        budget: Arc<SnapshotMountBudget>,
        options: super::PackedHistoryRetirementOptions,
    ) -> Result<super::PackedHistoryRetirementReport, WorkspaceError> {
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let permit = budget
            .admit(&[(SnapshotBudgetPool::Metadata, HISTORY_OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if options.incarnation.is_nil()
            || options.grace_ns == 0
            || options.grace_ns > MAX_HISTORY_GRACE_NS
            || options.max_native_holds == 0
            || options.max_current_bindings == 0
            || options.max_work == 0
        {
            return Err(WorkspaceError::Fenced);
        }
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = store
                .retire_packed_binding_history_owned(client, &budget, options)
                .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| journal_error("historical retirement driver stopped"))?
    }

    async fn retire_packed_binding_history_owned<O: ObjectBackend + Clone>(
        self: &Arc<Self>,
        client: ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
        options: PackedHistoryRetirementOptions,
    ) -> Result<PackedHistoryRetirementReport, WorkspaceError> {
        history_live(budget, &options.cancel)?;
        let mut read = self.history_retirement_read(options.incarnation).await?;
        if read
            .observation
            .as_ref()
            .is_some_and(|observation| observation.grace_ns != options.grace_ns)
        {
            // Grace is an immutable durable policy for this retirement. A new
            // caller cannot shorten it by resetting the observation clock.
            return Err(WorkspaceError::Fenced);
        }
        if read.root.state == RootState::BindingHistory {
            let native = self
                .native_packed_hold_census(
                    &read.binding.base_revision,
                    options.max_native_holds,
                    budget,
                    options.cancel.clone(),
                )
                .await?;
            let mut checks = read.checks.clone();
            history_merge(
                &mut checks,
                native.zero_owner_checks(self, &read.binding.base_revision, budget)?,
            )?;
            let pins = self.packed_reader_pin_roots().await?;
            history_merge(&mut checks, &pins.checks)?;
            let (journal_roots, journal_checks, _journal_owner) =
                self.scan_packed_journal_layer_roots().await?;
            history_merge(&mut checks, &journal_checks)?;
            for root in pins.native_roots.iter().chain(journal_roots.iter()) {
                if self
                    .hold_chain_contains(*root, &read.binding.base_revision, &[])
                    .await?
                {
                    return Err(WorkspaceError::Busy);
                }
            }
            let current = self
                .current_packed_history_census(
                    &read.binding,
                    &checks,
                    options.max_current_bindings,
                    budget,
                    &options.cancel,
                )
                .await?;
            if current.matched != 0 {
                return Err(WorkspaceError::Busy);
            }
            history_merge(&mut checks, &current.checks)?;
            let borrowed = self
                .borrowed_history_census(
                    &read.binding,
                    &checks,
                    options.max_current_bindings,
                    budget,
                    &options.cancel,
                )
                .await?;
            if borrowed.matched != 0 {
                return Err(WorkspaceError::Busy);
            }
            history_merge(&mut checks, &borrowed.checks)?;
            history_live(budget, &options.cancel)?;
            let generation = next_packed_root_generation(&read.generation)?;
            let registry_gate_digest = history_gate_digest(&checks, GATE_KEY)?;
            let native_gate_digest = history_gate_digest(&checks, HOLD_FEATURE)?;
            let observation_matches = read.observation.as_ref().is_some_and(|observation| {
                observation.phase == ObservationPhase::Observing
                    && observation.grace_ns == options.grace_ns
                    && observation.root_revision == read.root.revision
                    && observation.layer_generation == read.layer_generation
                    && observation.registry_gate_digest == registry_gate_digest
                    && observation.native_gate_digest == native_gate_digest
                    && encode(&observation.successor_generation).ok().as_ref()
                        == read.generation.as_ref()
            });
            if !observation_matches {
                let observation = Observation {
                    retirement_id: Uuid::new_v4(),
                    incarnation: read.root.incarnation,
                    binding_digest: Sha256::digest(read.binding.encode()?).into(),
                    root_revision: read.root.revision,
                    successor_generation: decode_required(&read.generation)?,
                    layer_generation: read.layer_generation,
                    registry_gate_digest,
                    native_gate_digest,
                    observed_at_ns: read.now,
                    grace_ns: options.grace_ns,
                    phase: ObservationPhase::Observing,
                };
                // Recording grace does not change roots. Keep the generation
                // in the exact CAS checks without invalidating other histories'
                // observations; actual retirement still advances it below.
                let writes = [KvWrite::Put {
                    key: observation_key(read.root.incarnation),
                    value: observation.encode()?,
                }];
                self.history_clock_cas(&checks, &writes, Some(read.now))
                    .await?;
                return Ok(PackedHistoryRetirementReport {
                    observing: true,
                    retired: false,
                    not_before_ns: observation.not_before_ns()?,
                    released_members: 0,
                    deleted_objects: 0,
                    quarantined_objects: 0,
                });
            }
            let mut observation = read.observation.clone().ok_or(WorkspaceError::Fenced)?;
            if !observation.matches(&read.root, &read.binding)? {
                return Err(WorkspaceError::Fenced);
            }
            let lower = observation.not_before_ns()?;
            if read.now < lower {
                self.history_clock_cas(&checks, &[], Some(read.now)).await?;
                return Ok(PackedHistoryRetirementReport {
                    observing: true,
                    retired: false,
                    not_before_ns: lower,
                    released_members: 0,
                    deleted_objects: 0,
                    quarantined_objects: 0,
                });
            }
            let mut root = read.root.clone();
            root.state = RootState::Retiring;
            root.revision = increment(root.revision)?;
            observation.phase = ObservationPhase::Committed;
            let mut writes = vec![
                KvWrite::Put {
                    key: registry_root_key(root.incarnation),
                    value: root.encode()?,
                },
                KvWrite::Put {
                    key: registry_history_root_key(&read.binding),
                    value: root.encode()?,
                },
                KvWrite::Delete {
                    key: packed_history_key(
                        read.binding.workspace_id,
                        read.binding.binding.binding_version,
                    ),
                },
                KvWrite::Put {
                    key: observation_key(root.incarnation),
                    value: observation.encode()?,
                },
                put(PACKED_ROOT_GENERATION_KEY.to_vec(), &generation)?,
            ];
            if current.drop_target_current {
                if read.current.as_deref() != Some(read.binding.encode()?.as_slice()) {
                    return Err(WorkspaceError::Fenced);
                }
                writes.push(KvWrite::Delete {
                    key: packed_current_key(read.binding.workspace_id),
                });
                writes.push(KvWrite::Delete {
                    key: packed_claim_key(read.binding.workspace_id),
                });
            }
            self.history_clock_cas(&checks, &writes, Some(lower))
                .await?;
            read = self.history_retirement_read(options.incarnation).await?;
        }
        let lower = read
            .observation
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?
            .not_before_ns()?;
        let mut remaining = options.max_work;
        let (deleted_objects, quarantined_objects, initial_queue_checks) = self
            .consume_history_delete_queue(&options, &client, budget, lower, &mut remaining)
            .await?;
        let mut report = PackedHistoryRetirementReport {
            observing: false,
            retired: false,
            not_before_ns: lower,
            released_members: 0,
            deleted_objects,
            quarantined_objects,
        };
        let Some(initial_queue_checks) = initial_queue_checks else {
            return Ok(report);
        };
        if read.root.state == RootState::Retired {
            let mut read = self.history_retirement_read(options.incarnation).await?;
            history_require_committed(&read, lower)?;
            history_merge(&mut read.checks, &initial_queue_checks)?;
            self.history_clock_cas(&read.checks, &[], Some(lower))
                .await?;
            report.retired = true;
            return Ok(report);
        }
        let prefix =
            format!("{REGISTRY_REVERSE_PREFIX}{}/", options.incarnation.simple()).into_bytes();
        let mut after = None;
        let mut visited = 0;
        loop {
            history_live(budget, &options.cancel)?;
            if remaining == 0 {
                return Ok(report);
            }
            let before = self.history_retirement_read(options.incarnation).await?;
            history_require_committed(&before, lower)?;
            self.history_clock_cas(&before.checks, &[], Some(lower))
                .await?;
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(&prefix, after.as_deref(), history_pages())
                .await?;
            self.history_clock_cas(&before.checks, &[], Some(lower))
                .await?;
            if page.is_empty() {
                break;
            }
            for entry in page {
                history_live(budget, &options.cancel)?;
                if remaining == 0 {
                    return Ok(report);
                }
                visited = increment(visited)?;
                if visited > MAX_OBJECTS {
                    return Err(WorkspaceError::Busy);
                }
                if !entry.key.starts_with(&prefix)
                    || after.as_ref().is_some_and(|last| entry.key <= *last)
                {
                    return Err(WorkspaceError::Fenced);
                }
                let reference =
                    SnapshotObjectRef::decode_value(&entry.value).map_err(journal_error)?;
                self.release_history_member(options.incarnation, &entry, &reference, lower)
                    .await?;
                remaining -= 1;
                report.released_members = increment(report.released_members)?;
                after = Some(entry.key);
            }
        }
        let (completed, quarantined, queue_checks) = self
            .consume_history_delete_queue(&options, &client, budget, lower, &mut remaining)
            .await?;
        report.deleted_objects = report
            .deleted_objects
            .checked_add(completed)
            .ok_or(WorkspaceError::Fenced)?;
        report.quarantined_objects = report
            .quarantined_objects
            .checked_add(quarantined)
            .ok_or(WorkspaceError::Fenced)?;
        let Some(queue_checks) = queue_checks else {
            return Ok(report);
        };
        history_live(budget, &options.cancel)?;
        let mut read = self.history_retirement_read(options.incarnation).await?;
        history_require_committed(&read, lower)?;
        // Finished consumes the exact epoch of the terminal empty queue page.
        history_merge(&mut read.checks, &queue_checks)?;
        if read.root.members != 0
            || read.root.pending_puts != 0
            || read.root.state != RootState::Retiring
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut root = read.root;
        root.state = RootState::Retired;
        root.revision = increment(root.revision)?;
        let mut observation = read.observation.ok_or(WorkspaceError::Fenced)?;
        observation.phase = ObservationPhase::Finished;
        let writes = vec![
            KvWrite::Put {
                key: registry_root_key(root.incarnation),
                value: root.encode()?,
            },
            KvWrite::Put {
                key: registry_history_root_key(&read.binding),
                value: root.encode()?,
            },
            KvWrite::Put {
                key: observation_key(root.incarnation),
                value: observation.encode()?,
            },
            put(
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                &next_packed_root_generation(&read.generation)?,
            )?,
        ];
        self.history_clock_cas(&read.checks, &writes, Some(lower))
            .await?;
        report.retired = true;
        Ok(report)
    }

    async fn release_history_member(
        &self,
        incarnation: Uuid,
        entry: &KvEntry,
        reference: &SnapshotObjectRef,
        lower: i64,
    ) -> Result<bool, WorkspaceError> {
        let mut read = self.history_retirement_read(incarnation).await?;
        history_require_committed(&read, lower)?;
        if read.root.state != RootState::Retiring {
            return Err(WorkspaceError::Fenced);
        }
        let keys = [
            registry_object_key(reference),
            registry_member_key(reference, incarnation),
            entry.key.clone(),
        ];
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, history_limits())
            .await?;
        if values.len() != 3 || values[2].as_deref() != Some(entry.value.as_slice()) {
            return Err(WorkspaceError::Busy);
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        let mut object = ObjectRow::decode(required(0)?)?;
        let mut member = MemberRow::decode(required(1)?)?;
        if object.reference != *reference
            || object.state != ObjectState::Live
            || member.reference != *reference
            || member.incarnation != incarnation
            || member.journal_id != read.root.journal_id
            || !member.retained
            || member.pending_put
            || entry.key != registry_reverse_key(incarnation, member.ordinal)
        {
            return Err(WorkspaceError::Fenced);
        }
        history_merge(
            &mut read.checks,
            &keys
                .into_iter()
                .zip(values)
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>(),
        )?;
        self.history_member_source_checks(&mut read, &member, reference)
            .await?;
        member.retained = false;
        object.memberships = decrement(object.memberships)?;
        object.revision = increment(object.revision)?;
        read.root.members = decrement(read.root.members)?;
        read.root.revision = increment(read.root.revision)?;
        let retiring = object.memberships == 0;
        if retiring {
            if object.pending_puts != 0 {
                return Err(WorkspaceError::Fenced);
            }
            object.state = ObjectState::Retiring;
        }
        let mut writes = vec![
            KvWrite::Put {
                key: registry_root_key(incarnation),
                value: read.root.encode()?,
            },
            KvWrite::Put {
                key: registry_history_root_key(&read.binding),
                value: read.root.encode()?,
            },
            KvWrite::Put {
                key: registry_object_key(reference),
                value: object.encode()?,
            },
            KvWrite::Put {
                key: registry_member_key(reference, incarnation),
                value: member.encode()?,
            },
            KvWrite::Delete {
                key: entry.key.clone(),
            },
            put(
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                &next_packed_root_generation(&read.generation)?,
            )?,
        ];
        if retiring {
            // Keep the final object's identity across cancellation after member
            // release and before DELETE reserve. Recovery never needs a global
            // object scan to find this root's already released delete work.
            let key = delete_queue_key(incarnation, member.ordinal);
            let (old, _) = self
                .backend
                .get_many_consistent_with_time_bounded(std::slice::from_ref(&key), history_limits())
                .await?;
            if old.len() != 1 || old[0].is_some() {
                return Err(WorkspaceError::Fenced);
            }
            history_merge(
                &mut read.checks,
                &[KvCheck {
                    key: key.clone(),
                    expected: None,
                }],
            )?;
            writes.push(KvWrite::Put {
                key,
                value: reference.encode_value().map_err(journal_error)?,
            });
        }
        self.history_clock_cas(&read.checks, &writes, Some(lower))
            .await?;
        Ok(retiring)
    }

    async fn consume_history_delete_queue<O: ObjectBackend + Clone>(
        &self,
        options: &PackedHistoryRetirementOptions,
        client: &ObjectClient<O>,
        budget: &Arc<SnapshotMountBudget>,
        lower: i64,
        remaining: &mut u64,
    ) -> Result<(u64, u64, Option<Vec<KvCheck>>), WorkspaceError> {
        let incarnation = options.incarnation;
        let cancel = &options.cancel;
        let prefix = format!(
            "packed/registry/history-delete-queue/{}/",
            incarnation.simple()
        )
        .into_bytes();
        let mut after = None;
        let mut deleted = 0;
        let mut quarantined = 0;
        let mut visited = 0;
        loop {
            history_live(budget, cancel)?;
            if *remaining == 0 {
                // Only a validated empty page can authorize final retirement.
                return Ok((deleted, quarantined, None));
            }
            let before = self.history_retirement_read(incarnation).await?;
            history_require_committed(&before, lower)?;
            if !matches!(before.root.state, RootState::Retiring | RootState::Retired) {
                return Err(WorkspaceError::Fenced);
            }
            self.history_clock_cas(&before.checks, &[], Some(lower))
                .await?;
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(&prefix, after.as_deref(), history_pages())
                .await?;
            self.history_clock_cas(&before.checks, &[], Some(lower))
                .await?;
            if page.is_empty() {
                history_live(budget, cancel)?;
                return Ok((deleted, quarantined, Some(before.checks)));
            }
            for entry in page {
                history_live(budget, cancel)?;
                if *remaining == 0 {
                    return Ok((deleted, quarantined, None));
                }
                visited = increment(visited)?;
                if visited > MAX_OBJECTS
                    || !entry.key.starts_with(&prefix)
                    || after.as_ref().is_some_and(|last| entry.key <= *last)
                {
                    return Err(WorkspaceError::Fenced);
                }
                let reference =
                    SnapshotObjectRef::decode_value(&entry.value).map_err(journal_error)?;
                let mut read = self.history_retirement_read(incarnation).await?;
                history_require_committed(&read, lower)?;
                let keys = [
                    entry.key.clone(),
                    registry_object_key(&reference),
                    registry_member_key(&reference, incarnation),
                ];
                let (values, _) = self
                    .backend
                    .get_many_consistent_with_time_bounded(&keys, history_limits())
                    .await?;
                if values.len() != 3 || values[0].as_deref() != Some(entry.value.as_slice()) {
                    return Err(WorkspaceError::Busy);
                }
                let member =
                    MemberRow::decode(values[2].as_deref().ok_or(WorkspaceError::Fenced)?)?;
                let mut object =
                    ObjectRow::decode(values[1].as_deref().ok_or(WorkspaceError::Fenced)?)?;
                if member.retained
                    || member.pending_put
                    || member.reference != reference
                    || member.incarnation != incarnation
                    || member.journal_id != read.root.journal_id
                    || entry.key != delete_queue_key(incarnation, member.ordinal)
                    || object.reference != reference
                    || object.memberships != 0
                    || object.pending_puts != 0
                {
                    return Err(WorkspaceError::Fenced);
                }
                if object.state == ObjectState::Retiring {
                    self.delete_registered_packed_object_for_history(
                        client,
                        reference.clone(),
                        budget,
                        cancel,
                        HistoryDeleteContext {
                            incarnation,
                            ordinal: member.ordinal,
                            lower,
                        },
                    )
                    .await?;
                    let (actual, _) = self
                        .backend
                        .get_many_consistent_with_time_bounded(
                            std::slice::from_ref(&keys[1]),
                            history_limits(),
                        )
                        .await?;
                    if actual.len() != 1 {
                        return Err(WorkspaceError::Fenced);
                    }
                    object =
                        ObjectRow::decode(actual[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
                    // DELETE itself advanced the common epoch; consume a fresh
                    // complete root/mapping basis before clearing the queue.
                    read = self.history_retirement_read(incarnation).await?;
                    history_require_committed(&read, lower)?;
                    let fresh_keys = [entry.key.clone(), keys[1].clone(), keys[2].clone()];
                    let (fresh, _) = self
                        .backend
                        .get_many_consistent_with_time_bounded(&fresh_keys, history_limits())
                        .await?;
                    if fresh.len() != 3
                        || fresh[0] != values[0]
                        || fresh[1] != actual[0]
                        || fresh[2] != values[2]
                    {
                        return Err(WorkspaceError::Busy);
                    }
                    history_merge(
                        &mut read.checks,
                        &fresh_keys
                            .into_iter()
                            .zip(fresh)
                            .map(|(key, expected)| KvCheck { key, expected })
                            .collect::<Vec<_>>(),
                    )?;
                } else {
                    history_merge(
                        &mut read.checks,
                        &keys
                            .into_iter()
                            .zip(values)
                            .map(|(key, expected)| KvCheck { key, expected })
                            .collect::<Vec<_>>(),
                    )?;
                }
                self.history_member_source_checks(&mut read, &member, &reference)
                    .await?;
                if object.reference != reference
                    || object.memberships != 0
                    || object.pending_puts != 0
                {
                    return Err(WorkspaceError::Fenced);
                }
                match object.state {
                    ObjectState::Deleted => deleted = increment(deleted)?,
                    // A pending remote outcome is quarantined by its durable
                    // single-use object guard. Never reserve or repeat DELETE.
                    ObjectState::DeletePending => quarantined = increment(quarantined)?,
                    _ => return Err(WorkspaceError::Fenced),
                }
                let writes = vec![
                    KvWrite::Delete {
                        key: entry.key.clone(),
                    },
                    put(
                        PACKED_ROOT_GENERATION_KEY.to_vec(),
                        &next_packed_root_generation(&read.generation)?,
                    )?,
                ];
                self.history_clock_cas(&read.checks, &writes, Some(lower))
                    .await?;
                *remaining -= 1;
                after = Some(entry.key);
            }
        }
    }
}

#[cfg(test)]
#[path = "history_retirement_tests.rs"]
mod tests;
