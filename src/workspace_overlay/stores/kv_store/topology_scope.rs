//! The entity catalog is the sole durable topology authority. ControlState is
//! a transient view over declared entity reads; it is never encoded to CONTROL.

use super::*;
use crate::workspace_overlay::packed_v3::wire005::V3OwnedPermit;
use crate::workspace_overlay::stores::kv_backend::KvReadLimits;

#[derive(Default)]
pub(super) struct TopologyScope {
    pub(super) workspaces: Vec<WorkspaceId>,
    pub(super) layers: Vec<LayerId>,
    pub(super) leases: Vec<(WorkspaceId, LeaseId)>,
    pub(super) journals: Vec<(WorkspaceId, JournalId)>,
    pub(super) snapshots: Vec<SnapshotId>,
    pub(super) allocators: Vec<String>,
    pub(super) lease_ids: Vec<LeaseId>,
    pub(super) journal_ids: Vec<JournalId>,
    pub(super) extra_keys: Vec<Vec<u8>>,
    pub(super) workspace_heads: bool,
    pub(super) sealed_ancestry: bool,
}

pub(super) struct ScopedTopology {
    pub(super) state: ControlState,
    pub(super) checks: Vec<KvCheck>,
    pub(super) now_ns: i64,
    original: ControlState,
}

pub(super) struct PreparedTopologyPacket {
    pub(super) checks: Vec<KvCheck>,
    pub(super) writes: Vec<KvWrite>,
    pub(super) deadline: Option<i64>,
    _owners: Vec<V3OwnedPermit>,
}

pub(super) fn topology_point_limits(records: usize) -> KvReadLimits {
    KvReadLimits {
        max_records: records,
        max_key_bytes: 1024,
        max_value_bytes: 48 << 10,
        max_total_bytes: 2 << 20,
        max_response_bytes: 128 << 10,
        max_data_requests: records,
    }
}

pub(super) fn validate_current_control_raw(
    raw: Option<&[u8]>,
) -> Result<ControlHeader, WorkspaceError> {
    let header = decode_control(raw.ok_or(WorkspaceError::Fenced)?)?;
    if header.schema_version != WORKSPACE_SCHEMA_VERSION {
        return Err(WorkspaceError::UnsupportedSchemaVersion(
            header.schema_version,
        ));
    }
    if header.catalog_format != CATALOG_FORMAT {
        return Err(WorkspaceError::CorruptMetadata(
            "unsupported catalog format".into(),
        ));
    }
    Ok(header)
}

fn merge_checks(
    target: &mut Vec<KvCheck>,
    added: impl IntoIterator<Item = KvCheck>,
) -> Result<(), WorkspaceError> {
    for check in added {
        if let Some(prior) = target.iter().find(|prior| prior.key == check.key) {
            if prior.expected != check.expected {
                return Err(WorkspaceError::Busy);
            }
        } else {
            target.push(check);
        }
    }
    Ok(())
}

fn is_entity_key(key: &[u8]) -> bool {
    [
        HOT_WORKSPACE_PREFIX,
        HOT_LAYER_PREFIX,
        HOT_LEASE_PREFIX,
        HOT_SNAPSHOT_PREFIX,
        HOT_JOURNAL_PREFIX,
        HOT_ALLOCATOR_PREFIX,
        LEASE_INDEX_PREFIX,
        JOURNAL_INDEX_PREFIX,
        SNAPSHOT_NAME_PREFIX,
    ]
    .iter()
    .any(|prefix| key.starts_with(prefix))
}

/// Decode only the declared entity bytes. Unknown auxiliary keys are ignored.
/// Missing entity keys remain exact absence checks in the caller's packet.
pub(super) fn topology_state_from_checks(
    checks: &[KvCheck],
) -> Result<ControlState, WorkspaceError> {
    let raw = checks
        .iter()
        .find(|check| check.key.as_slice() == CONTROL_KEY)
        .and_then(|check| check.expected.as_deref());
    let header = validate_current_control_raw(raw)?;
    let mut state = ControlState {
        schema_version: header.schema_version,
        header: header.header,
        ..ControlState::default()
    };
    for check in checks {
        let Some(raw) = check.expected.as_deref() else {
            continue;
        };
        let key = check.key.as_slice();
        if key.starts_with(HOT_WORKSPACE_PREFIX) {
            let row: WorkspaceRecord = decode_open_value(raw, 48 << 10)?;
            if hot_workspace_key(row.workspace_id) != key {
                return Err(WorkspaceError::Fenced);
            }
            state.workspaces.insert(row.workspace_id, row);
        } else if key.starts_with(HOT_LAYER_PREFIX) {
            let row: LayerRecord = decode_open_value(raw, 48 << 10)?;
            if hot_layer_key(row.layer_id) != key {
                return Err(WorkspaceError::Fenced);
            }
            state.layers.insert(row.layer_id, row);
        } else if key.starts_with(HOT_LEASE_PREFIX) {
            let row: SnapshotLease = decode_open_value(raw, 48 << 10)?;
            if hot_lease_key(row.workspace_id, row.lease_id) != key {
                return Err(WorkspaceError::Fenced);
            }
            if state.leases.insert(row.lease_id, row).is_some() {
                return Err(WorkspaceError::Fenced);
            }
        } else if key.starts_with(HOT_JOURNAL_PREFIX) {
            let row: SealJournal = decode_open_value(raw, 48 << 10)?;
            if hot_journal_key(row.workspace_id, row.journal_id) != key {
                return Err(WorkspaceError::Fenced);
            }
            if state.journals.insert(row.journal_id, row).is_some() {
                return Err(WorkspaceError::Fenced);
            }
        } else if key.starts_with(HOT_SNAPSHOT_PREFIX) {
            let row: SnapshotRecord = decode_open_value(raw, 48 << 10)?;
            if hot_snapshot_key(row.snapshot_id) != key {
                return Err(WorkspaceError::Fenced);
            }
            state.snapshots.insert(row.snapshot_id, row);
        } else if let Some(name) = key.strip_prefix(HOT_ALLOCATOR_PREFIX) {
            let name = String::from_utf8(name.to_vec()).map_err(|_| WorkspaceError::Fenced)?;
            state
                .allocators
                .insert(name, decode_open_value(raw, 48 << 10)?);
        }
    }
    Ok(state)
}

fn check_total(checks: &[KvCheck], limits: KvReadLimits) -> Result<(), WorkspaceError> {
    limits.validate()?;
    if checks.len() > limits.max_records || checks.len() > limits.max_data_requests {
        return Err(WorkspaceError::InvalidReadPlan(
            "topology scope record limit".into(),
        ));
    }
    let mut total = 0usize;
    for check in checks {
        let bytes = check.expected.as_ref().map_or(0, Vec::len);
        if check.key.len() > limits.max_key_bytes || bytes > limits.max_value_bytes {
            return Err(WorkspaceError::InvalidReadPlan(
                "topology scope row limit".into(),
            ));
        }
        total = total
            .checked_add(check.key.len())
            .and_then(|n| n.checked_add(bytes))
            .ok_or_else(|| WorkspaceError::InvalidReadPlan("topology scope overflow".into()))?;
    }
    if total > limits.max_total_bytes {
        return Err(WorkspaceError::InvalidReadPlan(
            "topology scope byte limit".into(),
        ));
    }
    Ok(())
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    pub(super) async fn read_topology_scope(
        &self,
        scope: &TopologyScope,
        limits: KvReadLimits,
    ) -> Result<ScopedTopology, WorkspaceError> {
        limits.validate()?;
        let mut keys = BTreeSet::from([CONTROL_KEY.to_vec()]);
        keys.extend(scope.workspaces.iter().copied().map(hot_workspace_key));
        keys.extend(scope.layers.iter().copied().map(hot_layer_key));
        keys.extend(scope.snapshots.iter().copied().map(hot_snapshot_key));
        keys.extend(scope.allocators.iter().map(|name| hot_allocator_key(name)));
        keys.extend(scope.extra_keys.iter().cloned());
        for &(workspace, id) in &scope.leases {
            keys.insert(hot_lease_key(workspace, id));
            keys.insert(lease_index_key(id));
        }
        for &(workspace, id) in &scope.journals {
            keys.insert(hot_journal_key(workspace, id));
            keys.insert(journal_index_key(id));
        }
        keys.extend(scope.lease_ids.iter().copied().map(lease_index_key));
        keys.extend(scope.journal_ids.iter().copied().map(journal_index_key));
        let mut checks = Vec::<KvCheck>::new();
        for _ in 0..=(LAYER_CHAIN_HARD_LIMIT as usize + 3) {
            let unread = keys
                .iter()
                .filter(|key| !checks.iter().any(|c| &c.key == *key))
                .cloned()
                .collect::<Vec<_>>();
            if keys.len() > limits.max_records || keys.len() > limits.max_data_requests {
                return Err(WorkspaceError::InvalidReadPlan(
                    "expanded topology scope exceeds plan".into(),
                ));
            }
            if unread.is_empty() {
                break;
            }
            for part in unread.chunks(32) {
                let read_limits = KvReadLimits {
                    max_records: part.len(),
                    ..limits
                };
                let (values, _) = self
                    .backend
                    .get_many_consistent_with_time_bounded(part, read_limits)
                    .await?;
                if values.len() != part.len() {
                    return Err(WorkspaceError::CorruptMetadata(
                        "short topology scope response".into(),
                    ));
                }
                merge_checks(
                    &mut checks,
                    part.iter()
                        .cloned()
                        .zip(values)
                        .map(|(key, expected)| KvCheck { key, expected }),
                )?;
            }
            check_total(&checks, limits)?;
            let state = topology_state_from_checks(&checks)?;
            for &id in &scope.lease_ids {
                if let Some(workspace) = checks
                    .iter()
                    .find(|c| c.key == lease_index_key(id))
                    .and_then(|c| c.expected.as_deref())
                    .map(decode::<WorkspaceId>)
                    .transpose()?
                {
                    keys.insert(hot_lease_key(workspace, id));
                }
            }
            for &id in &scope.journal_ids {
                if let Some(workspace) = checks
                    .iter()
                    .find(|c| c.key == journal_index_key(id))
                    .and_then(|c| c.expected.as_deref())
                    .map(decode::<WorkspaceId>)
                    .transpose()?
                {
                    keys.insert(hot_journal_key(workspace, id));
                }
            }
            if scope.workspace_heads {
                for row in state.workspaces.values() {
                    keys.insert(hot_layer_key(row.head_layer_id));
                    if let Some(base) = &row.fork_base {
                        keys.insert(hot_layer_key(base.layer_id));
                    }
                    if let Some(id) = row.active_lease {
                        keys.insert(hot_lease_key(row.workspace_id, id));
                        keys.insert(lease_index_key(id));
                    }
                }
            }
            if scope.sealed_ancestry {
                for row in state.leases.values() {
                    keys.insert(hot_layer_key(row.base_revision.layer_id));
                }
                for row in state.snapshots.values() {
                    keys.insert(hot_layer_key(row.revision.layer_id));
                }
                for row in state.journals.values() {
                    keys.insert(hot_layer_key(row.old_head_layer_id));
                    if let Some(id) = row.new_head_layer_id {
                        keys.insert(hot_layer_key(id));
                    }
                }
                for row in state.layers.values() {
                    if let Some(parent) = row.parent_layer_id {
                        keys.insert(hot_layer_key(parent));
                    }
                }
            }
            for row in state.snapshots.values() {
                if let Some(name) = &row.name {
                    keys.insert(snapshot_name_key(name));
                }
            }
        }
        if keys.len() != checks.len() {
            return Err(WorkspaceError::LayerDepthLimit {
                depth: LAYER_CHAIN_HARD_LIMIT + 1,
                hard_limit: LAYER_CHAIN_HARD_LIMIT,
            });
        }
        // Discovering keys grants no snapshot. The final fixed packet obtains
        // the time basis and all declared bytes together, with no limit growth.
        let final_keys = checks.iter().map(|c| c.key.clone()).collect::<Vec<_>>();
        let (values, now_ns) = self
            .backend
            .get_many_consistent_with_time_bounded(&final_keys, limits)
            .await?;
        if values.len() != checks.len() || now_ns <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        for (check, current) in checks.iter().zip(&values) {
            if &check.expected != current {
                return Err(WorkspaceError::Busy);
            }
        }
        check_total(&checks, limits)?;
        let state = topology_state_from_checks(&checks)?;
        validate_entity_indexes(&state, &checks, false)?;
        // An explicit absent entity must also have an absent global route.
        // Otherwise a new ID could overwrite another workspace's index.
        for &(workspace, id) in &scope.leases {
            validate_scoped_route(
                &checks,
                hot_lease_key(workspace, id),
                lease_index_key(id),
                workspace,
            )?;
        }
        for &(workspace, id) in &scope.journals {
            validate_scoped_route(
                &checks,
                hot_journal_key(workspace, id),
                journal_index_key(id),
                workspace,
            )?;
        }
        if scope.sealed_ancestry {
            for root in state.layers.keys() {
                let mut current = Some(*root);
                let mut seen = HashSet::new();
                while let Some(id) = current {
                    if !seen.insert(id) {
                        return Err(WorkspaceError::CorruptMetadata(
                            "scoped topology layer cycle".into(),
                        ));
                    }
                    check_depth(seen.len() as u32)?;
                    current = state
                        .layers
                        .get(&id)
                        .ok_or(WorkspaceError::LayerNotFound(id))?
                        .parent_layer_id;
                }
            }
        }
        Ok(ScopedTopology {
            original: state.clone(),
            state,
            checks,
            now_ns,
        })
    }

    pub(super) fn stage_topology_diff(
        &self,
        basis: &ScopedTopology,
        next: &ControlState,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
    ) -> Result<(), WorkspaceError> {
        merge_checks(checks, basis.checks.iter().cloned())?;
        for (id, row) in &next.leases {
            if !basis.original.leases.contains_key(id) {
                require_absent_route(checks, lease_index_key(*id))?;
                if row.lease_id != *id {
                    return Err(WorkspaceError::Fenced);
                }
            }
        }
        for (id, row) in &next.journals {
            if !basis.original.journals.contains_key(id) {
                require_absent_route(checks, journal_index_key(*id))?;
                if row.journal_id != *id {
                    return Err(WorkspaceError::Fenced);
                }
            }
        }
        let mut delta = Vec::new();
        append_hot_diff(&basis.original, next, &mut delta)?;
        append_entity_indexes(&basis.original, next, &mut delta)?;
        append_recovery_diff(&basis.original, next, &mut delta)?;
        if basis.original.header != next.header {
            delta.push(put_control(&ControlHeader {
                schema_version: next.schema_version,
                header: next.header.clone(),
                catalog_format: CATALOG_FORMAT,
            })?);
        }
        for write in &delta {
            let key = match write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
            };
            if !checks.iter().any(|check| &check.key == key) {
                return Err(WorkspaceError::Fenced);
            }
        }
        writes.extend(delta);
        Ok(())
    }

    pub(super) async fn prepare_topology_packet(
        &self,
        checks: Vec<KvCheck>,
        writes: Vec<KvWrite>,
        deadline: Option<i64>,
    ) -> Result<PreparedTopologyPacket, WorkspaceError> {
        self.prepare_topology_packet_inner(checks, writes, deadline, true)
            .await
    }

    /// Use when the driver has already prepared and still owns all native
    /// derived-write permits. This never repeats those preparations.
    pub(super) async fn prepare_topology_envelope(
        &self,
        checks: Vec<KvCheck>,
        writes: Vec<KvWrite>,
        deadline: Option<i64>,
    ) -> Result<PreparedTopologyPacket, WorkspaceError> {
        self.prepare_topology_packet_inner(checks, writes, deadline, false)
            .await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(writes = writes.len()))]
    async fn prepare_topology_packet_inner(
        &self,
        mut checks: Vec<KvCheck>,
        mut writes: Vec<KvWrite>,
        deadline: Option<i64>,
        prepare_native: bool,
    ) -> Result<PreparedTopologyPacket, WorkspaceError> {
        let control_check = checks
            .iter()
            .find(|check| check.key.as_slice() == CONTROL_KEY)
            .cloned();
        let control_raw = if let Some(check) = &control_check {
            check.expected.clone()
        } else {
            let (values, _) = self
                .backend
                .get_many_consistent_with_time_bounded(
                    &[CONTROL_KEY.to_vec()],
                    topology_point_limits(1),
                )
                .await?;
            if values.len() != 1 {
                return Err(WorkspaceError::Fenced);
            }
            values.into_iter().next().flatten()
        };
        validate_current_control_raw(control_raw.as_deref())?;
        let writes_control = writes.iter().any(|write| match write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => key.as_slice() == CONTROL_KEY,
        });
        if writes_control && control_check.is_none() {
            return Err(WorkspaceError::Fenced);
        }
        for write in &writes {
            if let KvWrite::Put { key, value } = write
                && key.as_slice() == CONTROL_KEY
            {
                validate_current_control_raw(Some(value))?;
            }
        }
        let layer_membership = writes.iter().any(|write| {
            let (key, present) = match write {
                KvWrite::Put { key, .. } => (key, true),
                KvWrite::Delete { key } => (key, false),
            };
            key.starts_with(HOT_LAYER_PREFIX)
                && checks
                    .iter()
                    .find(|check| check.key == *key)
                    .is_some_and(|check| check.expected.is_some() != present)
        });
        if layer_membership {
            advance_packet_generation(
                self,
                &mut checks,
                &mut writes,
                LAYER_INVENTORY_GENERATION_KEY,
            )
            .await?;
        }
        append_packet_recovery(self, &mut checks, &mut writes).await?;
        // The census epoch tracks membership and root shape. Sequence numbers,
        // allocator counters and lease heartbeats remain independent entity CAS.
        if changes_catalog_roots(&checks, &writes)? {
            let key = TOPOLOGY_GENERATION_KEY.to_vec();
            if !checks.iter().any(|check| check.key == key) {
                let (values, _) = self
                    .backend
                    .get_many_consistent_with_time_bounded(
                        std::slice::from_ref(&key),
                        topology_point_limits(1),
                    )
                    .await?;
                if values.len() != 1 {
                    return Err(WorkspaceError::Fenced);
                }
                merge_checks(
                    &mut checks,
                    [KvCheck {
                        key: key.clone(),
                        expected: values[0].clone(),
                    }],
                )?;
            }
            if !writes.iter().any(
                |write| matches!(write,KvWrite::Put{key:k,..}|KvWrite::Delete{key:k} if *k==key),
            ) {
                let raw = checks
                    .iter()
                    .find(|c| c.key == key)
                    .ok_or(WorkspaceError::Fenced)?
                    .expected
                    .clone();
                writes.push(put(key, &next_layer_inventory_generation(&raw)?)?);
            }
        }
        let mut owners = Vec::new();
        if prepare_native {
            if let Some(owner) = self
                .prepare_native_reverse_cas(&mut checks, &mut writes)
                .await?
            {
                owners.push(owner);
            }
            if let Some(owner) = self
                .prepare_native_owner_cas(&mut checks, &mut writes)
                .await?
            {
                owners.push(owner);
            }
            if let Some(owner) = self
                .prepare_native_extent_cas(&mut checks, &mut writes)
                .await?
            {
                owners.push(owner);
            }
        }
        // Reverse-index preparation validates every original dentry write and
        // derives its index from the last write for each name. Preserve that
        // namespace ordering here; other conflicting writes remain fenced.
        let mut unique = Vec::new();
        merge_checks(&mut unique, checks)?;
        let mut unique_writes = BTreeMap::new();
        for write in writes {
            let key = match &write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => key.clone(),
            };
            let ordered_dentry = key.starts_with(b"delta/dentry/");
            if let Some(prior) = unique_writes.insert(key, write.clone())
                && prior != write
                && !ordered_dentry
            {
                return Err(WorkspaceError::Fenced);
            }
        }
        Ok(PreparedTopologyPacket {
            checks: unique,
            writes: unique_writes.into_values().collect(),
            deadline,
            _owners: owners,
        })
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(super) async fn commit_prepared_topology_packet(
        &self,
        packet: &PreparedTopologyPacket,
    ) -> Result<bool, WorkspaceError> {
        match packet.deadline {
            Some(deadline) => {
                self.backend
                    .compare_and_swap_before(&packet.checks, &packet.writes, deadline)
                    .await
            }
            None => {
                self.backend
                    .compare_and_swap(&packet.checks, &packet.writes)
                    .await
            }
        }
    }

    pub(super) async fn confirm_prepared_topology_packet(
        &self,
        packet: &PreparedTopologyPacket,
    ) -> Result<bool, WorkspaceError> {
        let mut expected = packet.checks.clone();
        for write in &packet.writes {
            let (key, successor) = match write {
                KvWrite::Put { key, value } => (key, Some(value.clone())),
                KvWrite::Delete { key } => (key, None),
            };
            if let Some(check) = expected.iter_mut().find(|check| &check.key == key) {
                check.expected = successor;
            } else {
                expected.push(KvCheck {
                    key: key.clone(),
                    expected: successor,
                });
            }
        }
        // This is the exact successor of the complete attempted packet. The
        // operation's retained permit covers the receipt and confirmation bytes.
        match packet.deadline {
            Some(deadline) => {
                self.backend
                    .compare_and_swap_before(&expected, &[], deadline)
                    .await
            }
            None => self.backend.compare_and_swap(&expected, &[]).await,
        }
    }

    pub(super) async fn ensure_catalog_migration_quiescent(&self) -> Result<(), WorkspaceError> {
        // No old packed-v3 exact packet may be reinterpreted after key/schema
        // migration. Retained durable journals require explicit recovery first.
        for prefix in [
            b"packed/v3/journal/".as_slice(),
            b"packed/v3/journal-active/",
            b"packed/v3/reader-active/",
            b"packed/v3/reader-slot/",
            b"packed/v3/native-freeze-basis/",
            b"packed/v3/native-recovery-claim/",
            b"packed-v3/",
            b"open/v3/",
        ] {
            let limits = KvReadLimits {
                max_records: 1,
                max_key_bytes: 1024,
                max_value_bytes: 48 << 10,
                max_total_bytes: 64 << 10,
                max_response_bytes: 64 << 10,
                max_data_requests: 1,
            };
            if !self
                .backend
                .scan_prefix_page_with_byte_limits(prefix, None, limits)
                .await?
                .is_empty()
            {
                return Err(WorkspaceError::CorruptMetadata("catalog migration requires packed-v3 journals, readers and open sessions to be recovered and retired first".into()));
            }
        }
        Ok(())
    }

    pub(super) async fn read_complete_topology_census(
        &self,
    ) -> Result<
        (
            Option<Vec<u8>>,
            ControlState,
            BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        ),
        WorkspaceError,
    > {
        let keys = [
            CONTROL_KEY.to_vec(),
            TOPOLOGY_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
        ];
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, topology_point_limits(keys.len()))
            .await?;
        if values.len() != keys.len() {
            return Err(WorkspaceError::Fenced);
        }
        validate_current_control_raw(values[0].as_deref())?;
        let mut checks = keys
            .into_iter()
            .zip(values.clone())
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let mut count = 0usize;
        let mut bytes = 0usize;
        for prefix in [
            HOT_WORKSPACE_PREFIX,
            HOT_LAYER_PREFIX,
            HOT_LEASE_PREFIX,
            HOT_SNAPSHOT_PREFIX,
            HOT_JOURNAL_PREFIX,
            HOT_ALLOCATOR_PREFIX,
            LEASE_INDEX_PREFIX,
            JOURNAL_INDEX_PREFIX,
            SNAPSHOT_NAME_PREFIX,
        ] {
            let mut after = None;
            loop {
                let limits = KvReadLimits {
                    max_records: 32,
                    max_key_bytes: 1024,
                    max_value_bytes: 48 << 10,
                    max_total_bytes: 2 << 20,
                    max_response_bytes: 2 << 20,
                    max_data_requests: 32,
                };
                let page = self
                    .backend
                    .scan_prefix_page_with_byte_limits(prefix, after.as_deref(), limits)
                    .await?;
                if page.is_empty() {
                    break;
                }
                let page_bytes = validate_delta_scan_page(&page, prefix, after.as_deref())?;
                for entry in &page {
                    count = count.checked_add(1).ok_or(WorkspaceError::Fenced)?;
                    if count > 4096 {
                        return Err(WorkspaceError::InvalidReadPlan(
                            "administrative topology census limit".into(),
                        ));
                    }
                    checks.push(KvCheck {
                        key: entry.key.clone(),
                        expected: Some(entry.value.clone()),
                    });
                }
                bytes = bytes
                    .checked_add(page_bytes)
                    .ok_or(WorkspaceError::Fenced)?;
                if bytes > 32 << 20 {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "administrative topology census limit".into(),
                    ));
                }
                after = page.last().map(|entry| entry.key.clone());
            }
        }
        // This read does not grant GC authority by itself. Its exact checks and
        // epoch must be retained into the actual final reservation/deletion CAS.
        // The catalog header is validated for this snapshot, but it is not a
        // per-workspace mutation authority. Authenticate only the topology and
        // layer inventory generations here so unrelated workspace transitions
        // do not pessimistically lock CONTROL in Redis/TiKV.
        if !self.backend.compare_and_swap(&checks[1..3], &[]).await? {
            return Err(WorkspaceError::Busy);
        }
        let state = topology_state_from_checks(&checks)?;
        validate_entity_indexes(&state, &checks, true)?;
        validate_complete_census(&state)?;
        let raw = values[0].clone();
        let hot = checks
            .into_iter()
            .filter(|c| c.key.as_slice() != CONTROL_KEY)
            .map(|c| (c.key, c.expected))
            .collect();
        Ok((raw, state, hot))
    }
}

fn require_absent_route(checks: &[KvCheck], key: Vec<u8>) -> Result<(), WorkspaceError> {
    match checks.iter().find(|check| check.key == key) {
        Some(check) if check.expected.is_none() => Ok(()),
        _ => Err(WorkspaceError::Fenced),
    }
}

fn validate_scoped_route(
    checks: &[KvCheck],
    entity: Vec<u8>,
    index: Vec<u8>,
    workspace: WorkspaceId,
) -> Result<(), WorkspaceError> {
    let entity = checks
        .iter()
        .find(|check| check.key == entity)
        .ok_or(WorkspaceError::Fenced)?;
    let index = checks
        .iter()
        .find(|check| check.key == index)
        .ok_or(WorkspaceError::Fenced)?;
    let expected = if entity.expected.is_some() {
        Some(encode(&workspace)?)
    } else {
        None
    };
    if index.expected != expected {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
}

fn validate_entity_indexes(
    state: &ControlState,
    checks: &[KvCheck],
    complete: bool,
) -> Result<(), WorkspaceError> {
    let exact = |key: Vec<u8>, value: Vec<u8>| -> Result<(), WorkspaceError> {
        // Historical exact-row reads need no global-ID lookup. Explicit
        // mutable lease/journal targets already declare their index keys.
        if !complete && !checks.iter().any(|check| check.key == key) {
            return Ok(());
        }
        if checks
            .iter()
            .find(|check| check.key == key)
            .and_then(|check| check.expected.as_deref())
            != Some(value.as_slice())
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    };
    for row in state.leases.values() {
        exact(lease_index_key(row.lease_id), encode(&row.workspace_id)?)?;
    }
    for row in state.journals.values() {
        exact(
            journal_index_key(row.journal_id),
            encode(&row.workspace_id)?,
        )?;
    }
    for row in state.snapshots.values() {
        if let Some(name) = &row.name {
            exact(snapshot_name_key(name), encode(&row.snapshot_id)?)?;
        }
    }
    Ok(())
}

fn validate_complete_census(state: &ControlState) -> Result<(), WorkspaceError> {
    let mut roots = Vec::new();
    for row in state.workspaces.values() {
        if row.state != WorkspaceState::Deleting {
            roots.push(row.head_layer_id);
            if let Some(base) = &row.fork_base {
                roots.push(base.layer_id);
            }
        }
    }
    roots.extend(state.snapshots.values().map(|row| row.revision.layer_id));
    roots.extend(
        state
            .leases
            .values()
            .filter(|row| matches!(row.state, LeaseState::Active | LeaseState::Releasing))
            .map(|row| row.base_revision.layer_id),
    );
    roots.extend(
        state
            .journals
            .values()
            .filter(|row| !matches!(row.phase, SealPhase::Completed | SealPhase::Aborted))
            .map(|row| row.old_head_layer_id),
    );
    for root in roots {
        let mut current = Some(root);
        let mut seen = HashSet::new();
        while let Some(id) = current {
            if !seen.insert(id) {
                return Err(WorkspaceError::CorruptMetadata(
                    "topology census layer cycle".into(),
                ));
            }
            check_depth(seen.len() as u32)?;
            current = state
                .layers
                .get(&id)
                .ok_or(WorkspaceError::LayerNotFound(id))?
                .parent_layer_id;
        }
    }
    Ok(())
}

async fn advance_packet_generation<B: WorkspaceKvBackend>(
    store: &KvWorkspaceStore<B>,
    checks: &mut Vec<KvCheck>,
    writes: &mut Vec<KvWrite>,
    key: &[u8],
) -> Result<(), WorkspaceError> {
    if !checks.iter().any(|check| check.key == key) {
        let (values, _) = store
            .backend
            .get_many_consistent_with_time_bounded(&[key.to_vec()], topology_point_limits(1))
            .await?;
        if values.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        merge_checks(
            checks,
            [KvCheck {
                key: key.to_vec(),
                expected: values[0].clone(),
            }],
        )?;
    }
    if !writes
        .iter()
        .any(|write| matches!(write,KvWrite::Put{key:k,..}|KvWrite::Delete{key:k} if k==key))
    {
        let raw = checks
            .iter()
            .find(|check| check.key == key)
            .ok_or(WorkspaceError::Fenced)?
            .expected
            .clone();
        writes.push(put(key.to_vec(), &next_layer_inventory_generation(&raw)?)?);
    }
    Ok(())
}

async fn append_packet_recovery<B: WorkspaceKvBackend>(
    store: &KvWorkspaceStore<B>,
    checks: &mut Vec<KvCheck>,
    writes: &mut Vec<KvWrite>,
) -> Result<(), WorkspaceError> {
    let mut changes = BTreeMap::new();
    for write in writes.iter() {
        let (key, next) = match write {
            KvWrite::Put { key, value } => (key, Some(value.as_slice())),
            KvWrite::Delete { key } => (key, None),
        };
        if !key.starts_with(HOT_JOURNAL_PREFIX) {
            continue;
        }
        let old = checks
            .iter()
            .find(|check| check.key == *key)
            .ok_or(WorkspaceError::Fenced)?
            .expected
            .as_deref()
            .map(decode::<SealJournal>)
            .transpose()?;
        let next = next.map(decode::<SealJournal>).transpose()?;
        let live =
            |row: &SealJournal| !matches!(row.phase, SealPhase::Completed | SealPhase::Aborted);
        if old.as_ref().is_some_and(live) == next.as_ref().is_some_and(live) {
            continue;
        }
        let workspace = next
            .as_ref()
            .or(old.as_ref())
            .ok_or(WorkspaceError::Fenced)?
            .workspace_id;
        let incomplete = next.as_ref().is_some_and(live);
        if let Some(prior) = changes.insert(workspace, incomplete)
            && prior != incomplete
        {
            return Err(WorkspaceError::Fenced);
        }
    }
    for (workspace, incomplete) in changes {
        let key = open_v3_recovery_key(workspace);
        if !checks.iter().any(|check| check.key == key) {
            let (values, _) = store
                .backend
                .get_many_consistent_with_time_bounded(
                    std::slice::from_ref(&key),
                    topology_point_limits(1),
                )
                .await?;
            if values.len() != 1 {
                return Err(WorkspaceError::Fenced);
            }
            merge_checks(
                checks,
                [KvCheck {
                    key: key.clone(),
                    expected: values[0].clone(),
                }],
            )?;
        }
        if !writes
            .iter()
            .any(|write| matches!(write,KvWrite::Put{key:k,..}|KvWrite::Delete{key:k} if *k==key))
        {
            writes.push(if incomplete {
                put(
                    key,
                    &V3RecoveryRecord {
                        workspace_id: workspace,
                        incomplete: true,
                    },
                )?
            } else {
                KvWrite::Delete { key }
            });
        }
    }
    Ok(())
}

pub(super) async fn add_missing_write_checks<B: WorkspaceKvBackend>(
    store: &KvWorkspaceStore<B>,
    checks: &mut Vec<KvCheck>,
    writes: &[KvWrite],
) -> Result<(), WorkspaceError> {
    let keys = writes
        .iter()
        .filter_map(|write| {
            let key = match write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
            };
            (is_entity_key(key) && !checks.iter().any(|c| &c.key == key)).then(|| key.clone())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    for part in keys.chunks(32) {
        let (values, _) = store
            .backend
            .get_many_consistent_with_time_bounded(part, topology_point_limits(part.len()))
            .await?;
        if values.len() != part.len() {
            return Err(WorkspaceError::Fenced);
        }
        if values.iter().any(Option::is_some) {
            // The complete census did not contain this key. It must still be
            // absent, including the uniqueness/index key of a newly born row.
            return Err(WorkspaceError::Busy);
        }
        merge_checks(
            checks,
            part.iter()
                .cloned()
                .zip(values)
                .map(|(key, expected)| KvCheck { key, expected }),
        )?;
    }
    Ok(())
}

pub(super) fn append_entity_indexes(
    before: &ControlState,
    after: &ControlState,
    writes: &mut Vec<KvWrite>,
) -> Result<(), WorkspaceError> {
    for (id, row) in &before.leases {
        if !after.leases.contains_key(id) {
            writes.push(KvWrite::Delete {
                key: lease_index_key(*id),
            });
        } else if after.leases[id].workspace_id != row.workspace_id {
            return Err(WorkspaceError::Fenced);
        }
    }
    for (id, row) in &after.leases {
        if !before.leases.contains_key(id) {
            writes.push(put(lease_index_key(*id), &row.workspace_id)?);
        }
    }
    for (id, row) in &before.journals {
        if !after.journals.contains_key(id) {
            writes.push(KvWrite::Delete {
                key: journal_index_key(*id),
            });
        } else if after.journals[id].workspace_id != row.workspace_id {
            return Err(WorkspaceError::Fenced);
        }
    }
    for (id, row) in &after.journals {
        if !before.journals.contains_key(id) {
            writes.push(put(journal_index_key(*id), &row.workspace_id)?);
        }
    }
    for (id, row) in &before.snapshots {
        if let Some(name) = &row.name
            && after.snapshots.get(id).and_then(|r| r.name.as_ref()) != Some(name)
        {
            writes.push(KvWrite::Delete {
                key: snapshot_name_key(name),
            });
        }
    }
    for (id, row) in &after.snapshots {
        if let Some(name) = &row.name
            && before.snapshots.get(id).and_then(|r| r.name.as_ref()) != Some(name)
        {
            writes.push(put(snapshot_name_key(name), id)?);
        }
    }
    Ok(())
}

fn changes_catalog_roots(checks: &[KvCheck], writes: &[KvWrite]) -> Result<bool, WorkspaceError> {
    for write in writes {
        let (key, next) = match write {
            KvWrite::Put { key, value } => (key, Some(value.as_slice())),
            KvWrite::Delete { key } => (key, None),
        };
        if ![
            HOT_WORKSPACE_PREFIX,
            HOT_LAYER_PREFIX,
            HOT_LEASE_PREFIX,
            HOT_SNAPSHOT_PREFIX,
            HOT_JOURNAL_PREFIX,
        ]
        .iter()
        .any(|p| key.starts_with(p))
        {
            continue;
        }
        let old = checks
            .iter()
            .find(|check| check.key == *key)
            .ok_or(WorkspaceError::Fenced)?
            .expected
            .as_deref();
        if old.is_none() != next.is_none() {
            return Ok(true);
        }
        let (Some(old), Some(next)) = (old, next) else {
            continue;
        };
        if key.starts_with(HOT_WORKSPACE_PREFIX) {
            let a: WorkspaceRecord = decode(old)?;
            let b: WorkspaceRecord = decode(next)?;
            if a.head_layer_id != b.head_layer_id
                || a.fork_base != b.fork_base
                || a.state != b.state
                || a.active_lease != b.active_lease
            {
                return Ok(true);
            }
        } else if key.starts_with(HOT_LAYER_PREFIX) {
            let a: LayerRecord = decode(old)?;
            let b: LayerRecord = decode(next)?;
            if a.parent_layer_id != b.parent_layer_id
                || a.state != b.state
                || a.sealed_version != b.sealed_version
                || a.root_hash != b.root_hash
            {
                return Ok(true);
            }
        } else if key.starts_with(HOT_LEASE_PREFIX) {
            let a: SnapshotLease = decode(old)?;
            let b: SnapshotLease = decode(next)?;
            if a.base_revision != b.base_revision || a.state != b.state {
                return Ok(true);
            }
        } else if key.starts_with(HOT_JOURNAL_PREFIX) {
            let a: SealJournal = decode(old)?;
            let b: SealJournal = decode(next)?;
            if a.old_head_layer_id != b.old_head_layer_id
                || a.new_head_layer_id != b.new_head_layer_id
                || matches!(a.phase, SealPhase::Completed | SealPhase::Aborted)
                    != matches!(b.phase, SealPhase::Completed | SealPhase::Aborted)
            {
                return Ok(true);
            }
        } else if old != next {
            return Ok(true);
        }
    }
    Ok(false)
}
