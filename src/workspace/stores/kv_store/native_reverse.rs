//! Exact native aliases. Forward rows remain authoritative; completeness is
//! established by a persisted, fenced Building -> empty page -> Ready build.

use super::*;
use crate::workspace::packed::snapshot::{
    SnapshotBudgetPool, SnapshotMountBudget, SnapshotOwnedPermit,
};
use crate::workspace::stores::kv_backend::KvReadLimits;

const ROW_BYTES: usize = 4096;
const POINT_ROWS: usize = 32;
const BUILD_ROWS: usize = 16;
const MAX_MUTATION_ROWS: usize = 32768;
const MAX_MUTATION_BYTES: usize = 8 << 20;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum Phase {
    Building {
        after: Option<Vec<u8>>,
        inventory: Option<Vec<u8>>,
    },
    Ready,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct State {
    layer: LayerId,
    build: uuid::Uuid,
    phase: Phase,
}

pub(super) fn layer_prefix(layer: LayerId) -> Vec<u8> {
    format!("packed/native-reverse/rows/{layer}/").into_bytes()
}

pub(super) fn state_key(layer: LayerId) -> Vec<u8> {
    format!("packed/native-reverse/state/{layer}").into_bytes()
}

fn inode_prefix(layer: LayerId, build: uuid::Uuid, ino: i64) -> Vec<u8> {
    let mut key = layer_prefix(layer);
    key.extend_from_slice(format!("{build}/{}/", ino_component(ino)).as_bytes());
    key
}

fn row_key(state: &State, row: &DentryDelta) -> Result<Vec<u8>, WorkspaceError> {
    validate_row(row, &dentry_key(row), state.layer)?;
    if row.op != DentryOp::Put {
        return Err(corrupt("whiteout in native reverse index"));
    }
    let mut key = inode_prefix(state.layer, state.build, row.ino.unwrap());
    key.extend_from_slice(format!("{}/", ino_component(row.parent_ino)).as_bytes());
    key.extend_from_slice(hex::encode(&row.name).as_bytes());
    Ok(key)
}

fn valid_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name != b"."
        && name != b".."
        && !name.contains(&0)
        && !name.contains(&b'/')
}

fn corrupt(message: &str) -> WorkspaceError {
    WorkspaceError::CorruptMetadata(message.into())
}

fn limits(records: usize) -> KvReadLimits {
    KvReadLimits {
        max_records: records,
        max_key_bytes: 1024,
        max_value_bytes: ROW_BYTES,
        max_total_bytes: records * (1024 + ROW_BYTES),
        max_response_bytes: 256 << 10,
        max_data_requests: records,
    }
}

fn parse_dentry_key(key: &[u8]) -> Result<(LayerId, i64, Vec<u8>), WorkspaceError> {
    if key.len() > 1024 {
        return Err(corrupt("native reverse primary key limit"));
    }
    let suffix = key
        .strip_prefix(b"delta/dentry/")
        .ok_or_else(|| corrupt("native reverse primary prefix"))?;
    let fields = suffix.split(|byte| *byte == b'/').collect::<Vec<_>>();
    if fields.len() != 3 {
        return Err(corrupt("native reverse primary identity"));
    }
    let layer: LayerId = std::str::from_utf8(fields[0])
        .map_err(|_| corrupt("native reverse layer encoding"))?
        .parse()
        .map_err(|_| corrupt("native reverse layer identity"))?;
    let parent = u64::from_str_radix(
        std::str::from_utf8(fields[1]).map_err(|_| corrupt("native reverse parent encoding"))?,
        16,
    )
    .map_err(|_| corrupt("native reverse parent identity"))?
        ^ (1_u64 << 63);
    let name = hex::decode(fields[2]).map_err(|_| corrupt("native reverse name encoding"))?;
    let parent = parent as i64;
    if parent <= 0 || !valid_name(&name) || dentry_identity_key(layer, parent, &name) != key {
        return Err(corrupt("native reverse noncanonical primary identity"));
    }
    Ok((layer, parent, name))
}

fn validate_row(row: &DentryDelta, key: &[u8], layer: LayerId) -> Result<(), WorkspaceError> {
    row.validate()?;
    if row.layer_id != layer
        || row.parent_ino <= 0
        || row.sequence == 0
        || !valid_name(&row.name)
        || row.ino.is_some_and(|ino| ino <= 0)
        || dentry_key(row) != key
    {
        return Err(corrupt("native reverse primary row/key mismatch"));
    }
    Ok(())
}

fn decode_state(raw: &[u8], layer: LayerId) -> Result<State, WorkspaceError> {
    let state: State = decode_open_value(raw, ROW_BYTES)?;
    if state.layer != layer || state.build.is_nil() || encode(&state)? != raw {
        return Err(corrupt("native reverse state identity"));
    }
    if let Phase::Building { after, inventory } = &state.phase {
        layer_inventory_generation(inventory)?;
        if let Some(after) = after {
            let (cursor_layer, _, _) = parse_dentry_key(after)?;
            if cursor_layer != layer {
                return Err(corrupt("native reverse cursor layer"));
            }
        }
    }
    Ok(state)
}

/// A canonical build is a stable incarnation while the layer is Deleting.
/// This exposes identity only; Building never grants reverse completeness.
pub(super) fn gc_incarnation(raw: &[u8], layer: LayerId) -> Result<uuid::Uuid, WorkspaceError> {
    Ok(decode_state(raw, layer)?.build)
}

fn insert_derived(
    rows: &mut BTreeMap<Vec<u8>, KvWrite>,
    bytes: &mut usize,
    write: KvWrite,
) -> Result<(), WorkspaceError> {
    let key = write_key(&write);
    // Conservatively account overwritten entries as well. Returning an error
    // submits neither the forward change nor an incomplete reverse change.
    *bytes = bytes
        .checked_add(
            256 + key.len() * 2
                + match &write {
                    KvWrite::Put { value, .. } => value.len(),
                    KvWrite::Delete { .. } => 0,
                },
        )
        .filter(|bytes| *bytes <= MAX_MUTATION_BYTES)
        .ok_or_else(|| {
            WorkspaceError::InvalidReadPlan("native reverse derived allocation limit".into())
        })?;
    rows.insert(key.to_vec(), write);
    Ok(())
}

fn write_key(write: &KvWrite) -> &[u8] {
    match write {
        KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    fn reverse_admission(
        &self,
        rows: usize,
    ) -> Result<Option<SnapshotOwnedPermit>, WorkspaceError> {
        // Small namespace changes must coexist with the existing 32 MiB
        // mutation owner under the minimum 44 MiB mount metadata profile.
        // Larger packets retain at most 8 MiB derived + 8 MiB proof bytes;
        // the 32 MiB ceiling includes map/vector/transport expansion.
        let bytes = (1u64 << 20)
            .saturating_add((rows as u64).saturating_mul(12 << 10))
            .min(32 << 20);
        self.native_auxiliary_budget()
            .admit(&[(SnapshotBudgetPool::Metadata, bytes)])
            .map(Some)
            .map_err(|_| WorkspaceError::Io(std::io::Error::from_raw_os_error(libc::ENOMEM)))
    }

    async fn reverse_points(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        if keys.is_empty() || keys.len() > POINT_ROWS {
            return Err(WorkspaceError::InvalidReadPlan(
                "native reverse point packet".into(),
            ));
        }
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(keys, limits(keys.len()))
            .await?;
        if values.len() != keys.len() {
            return Err(corrupt("short native reverse authority packet"));
        }
        Ok(values)
    }

    /// All supported primary writers must call this before their actual CAS.
    /// Its owner survives the CAS. None is checked too, fencing a writer that
    /// prepared before an administrator installed Building. Deployment must
    /// stop older binaries that do not participate in this protocol.
    pub(super) async fn prepare_native_reverse_cas(
        &self,
        checks: &mut Vec<KvCheck>,
        writes: &mut Vec<KvWrite>,
    ) -> Result<Option<SnapshotOwnedPermit>, WorkspaceError> {
        if !writes.iter().any(|write| {
            let key = write_key(write);
            key.starts_with(b"delta/dentry/")
                || (key.starts_with(HOT_LAYER_PREFIX)
                    && (matches!(write, KvWrite::Delete { .. })
                        || checks
                            .iter()
                            .any(|check| check.key == key && check.expected.is_none())))
        }) {
            return Ok(None);
        }
        let count = writes
            .iter()
            .filter(|write| write_key(write).starts_with(b"delta/dentry/"))
            .count();
        if count > MAX_MUTATION_ROWS {
            return Err(WorkspaceError::InvalidReadPlan(
                "native reverse mutation input count".into(),
            ));
        }
        let owner = self.reverse_admission(count)?;
        let mut primary = BTreeMap::new();
        let mut layers = BTreeSet::new();
        let mut births = BTreeSet::new();
        let mut deletions = BTreeSet::new();
        for write in writes.iter() {
            let key = write_key(write);
            if key.starts_with(b"delta/dentry/") {
                let (layer, _, _) = parse_dentry_key(key)?;
                if let KvWrite::Put { value, .. } = write {
                    let row: DentryDelta = decode_open_value(value, ROW_BYTES)?;
                    validate_row(&row, key, layer)?;
                }
                // Repeated namespace/compaction names use their final write.
                primary.insert(key, write);
                layers.insert(layer);
            } else if key.starts_with(HOT_LAYER_PREFIX)
                && let Some(check) = checks.iter().find(|check| check.key == key)
            {
                match write {
                    KvWrite::Put { value, .. } if check.expected.is_none() => {
                        let row: LayerRecord = decode_open_value(value, ROW_BYTES)?;
                        if hot_layer_key(row.layer_id) != key {
                            return Err(corrupt("native reverse birth identity"));
                        }
                        // record_orphan_slice legitimately creates a fresh
                        // Deleting layer. Its empty-forward proof and build
                        // are the same; query authority rejects Deleting.
                        births.insert(row.layer_id);
                        layers.insert(row.layer_id);
                    }
                    KvWrite::Delete { .. } => {
                        let suffix = key
                            .strip_prefix(HOT_LAYER_PREFIX)
                            .ok_or(WorkspaceError::Busy)?;
                        let layer: LayerId = std::str::from_utf8(suffix)
                            .map_err(|_| corrupt("native reverse deletion layer"))?
                            .parse()
                            .map_err(|_| corrupt("native reverse deletion identity"))?;
                        if hot_layer_key(layer) != key {
                            return Err(corrupt("native reverse deletion key"));
                        }
                        if let Some(raw) = check.expected.as_deref() {
                            let row: LayerRecord = decode_open_value(raw, ROW_BYTES)?;
                            if row.layer_id != layer || row.state != LayerState::Deleting {
                                return Err(WorkspaceError::Busy);
                            }
                        }
                        deletions.insert(layer);
                        layers.insert(layer);
                    }
                    _ => {}
                }
            }
        }
        if layers.is_empty() {
            return Ok(owner);
        }
        if layers.len() > POINT_ROWS || primary.len() > MAX_MUTATION_ROWS {
            return Err(WorkspaceError::InvalidReadPlan(
                "native reverse mutation count".into(),
            ));
        }
        if (!births.is_empty() || !deletions.is_empty())
            && !checks
                .iter()
                .any(|check| check.key.as_slice() == LAYER_INVENTORY_GENERATION_KEY)
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "native reverse birth/deletion inventory authority".into(),
            ));
        }
        let keys = layers.iter().copied().map(state_key).collect::<Vec<_>>();
        // A caller may have read these exact keys in its own consistent view
        // read. The final CAS still checks every value, so a prefetched value
        // that is stale can only fail that CAS, never commit wrong state.
        let values = match super::prefetched_points(&keys) {
            Some(values) => values,
            None => self.reverse_points(&keys).await?,
        };
        let mut states = BTreeMap::new();
        let mut derived = BTreeMap::new();
        let mut derived_bytes = 0usize;
        // This helper owns only its additional proofs. CONTROL/publication
        // envelopes and their existing owners remain the caller's contract.
        let mut proof_bytes = 0usize;
        for ((layer, key), expected) in layers.iter().zip(keys).zip(values) {
            let state = expected
                .as_deref()
                .map(|raw| decode_state(raw, *layer))
                .transpose()?;
            proof_bytes = proof_bytes
                .checked_add(key.len() + expected.as_ref().map_or(0, Vec::len))
                .filter(|bytes| *bytes <= MAX_MUTATION_BYTES)
                .ok_or_else(|| {
                    WorkspaceError::InvalidReadPlan("native reverse retained proof limit".into())
                })?;
            checks.push(KvCheck {
                key: key.clone(),
                expected,
            });
            if deletions.contains(layer) {
                insert_derived(&mut derived, &mut derived_bytes, KvWrite::Delete { key })?;
            } else if births.contains(layer) {
                if state.is_some() {
                    return Err(WorkspaceError::Busy);
                }
                // Exact hot absence + inventory CAS fences every supported
                // insertion. A fresh UUID cannot reuse any abandoned build.
                let page = self
                    .backend
                    .scan_prefix_page_with_byte_limits(
                        &dentry_layer_prefix(*layer),
                        None,
                        limits(1),
                    )
                    .await?;
                if !page.is_empty() {
                    return Err(WorkspaceError::Busy);
                }
                let state = State {
                    layer: *layer,
                    build: uuid::Uuid::new_v4(),
                    phase: Phase::Ready,
                };
                insert_derived(&mut derived, &mut derived_bytes, put(key, &state)?)?;
                states.insert(*layer, state);
            } else if let Some(state) = state {
                states.insert(*layer, state);
            }
        }
        let indexed = primary
            .iter()
            .filter(|(key, _)| {
                parse_dentry_key(key).is_ok_and(|(layer, _, _)| states.contains_key(&layer))
            })
            .collect::<Vec<_>>();
        for chunk in indexed.chunks(POINT_ROWS) {
            let keys = chunk
                .iter()
                .map(|(key, _)| key.to_vec())
                .collect::<Vec<_>>();
            let values = self.reverse_points(&keys).await?;
            for (((key, write), primary_key), expected) in chunk.iter().zip(keys).zip(values) {
                let (layer, _, _) = parse_dentry_key(key)?;
                let state = &states[&layer];
                if let Some(raw) = &expected {
                    let old: DentryDelta = decode_open_value(raw, ROW_BYTES)?;
                    validate_row(&old, &primary_key, layer)?;
                    if old.op == DentryOp::Put {
                        let key = row_key(state, &old)?;
                        insert_derived(&mut derived, &mut derived_bytes, KvWrite::Delete { key })?;
                    }
                }
                proof_bytes = proof_bytes
                    .checked_add(primary_key.len() + expected.as_ref().map_or(0, Vec::len))
                    .filter(|bytes| *bytes <= MAX_MUTATION_BYTES)
                    .ok_or_else(|| {
                        WorkspaceError::InvalidReadPlan(
                            "native reverse retained proof limit".into(),
                        )
                    })?;
                checks.push(KvCheck {
                    key: primary_key,
                    expected,
                });
                if let KvWrite::Put { value, .. } = write {
                    let row: DentryDelta = decode_open_value(value, ROW_BYTES)?;
                    if row.op == DentryOp::Put {
                        let key = row_key(state, &row)?;
                        insert_derived(&mut derived, &mut derived_bytes, put(key, &row)?)?;
                    }
                }
            }
        }
        let total = derived.values().try_fold(0usize, |sum, write| {
            sum.checked_add(write_key(write).len())
                .and_then(|sum| {
                    sum.checked_add(match write {
                        KvWrite::Put { value, .. } => value.len(),
                        KvWrite::Delete { .. } => 0,
                    })
                })
                .ok_or_else(|| {
                    WorkspaceError::InvalidReadPlan("native reverse mutation bytes overflow".into())
                })
        })?;
        if total > MAX_MUTATION_BYTES
            || proof_bytes > MAX_MUTATION_BYTES
            || derived.len() > MAX_MUTATION_ROWS
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "native reverse mutation aggregate limit".into(),
            ));
        }
        // One final sort folds duplicate authority keys without quadratic
        // repeated scans of a large admitted mutation packet.
        checks.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        let mut conflict = false;
        checks.dedup_by(|later, earlier| {
            if later.key != earlier.key {
                return false;
            }
            conflict |= later.expected != earlier.expected;
            true
        });
        if conflict {
            return Err(WorkspaceError::Busy);
        }
        writes.extend(derived.into_values());
        Ok(owner)
    }

    fn reverse_budget(&self, budget: &Arc<SnapshotMountBudget>) -> Result<(), WorkspaceError> {
        if self
            .packed_reader_pin_budget
            .get()
            .is_none_or(|owned| !Arc::ptr_eq(owned, budget))
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "native reverse budget identity".into(),
            ));
        }
        Ok(())
    }

    /// Explicit admin upgrade for an old Deleting target without reverse state.
    /// Existing states are never replaced. No namespace scan or Ready claim is
    /// made: the fresh Building build provides only a durable incarnation.
    pub async fn initialize_native_reverse_deleting_identity(
        &self,
        layer: LayerId,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<(), WorkspaceError> {
        self.require_admin_access()?;
        self.reverse_budget(&budget)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 1 << 20)])
            .map_err(|_| WorkspaceError::Busy)?;
        let keys = [
            VOLUME_HEADER_KEY.to_vec(),
            hot_layer_key(layer),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            state_key(layer),
            CONTROL_KEY.to_vec(),
        ];
        let values = self.reverse_points(&keys).await?;
        validate_current_control_raw(values[4].as_deref())?;
        let header_raw = values[0].as_ref().ok_or(WorkspaceError::Fenced)?;
        let header: VolumeHeader = decode_open_value(header_raw, ROW_BYTES)?;
        if header.volume_id.is_nil()
            || header.volume_format != VOLUME_FORMAT
            || header.schema_version != WORKSPACE_SCHEMA_VERSION
            || header.created_at_ns <= 0
            || encode(&header)? != *header_raw
        {
            return Err(WorkspaceError::Fenced);
        }
        let raw = values[1]
            .as_ref()
            .ok_or(WorkspaceError::LayerNotFound(layer))?;
        let record: LayerRecord = decode_open_value(raw, ROW_BYTES)?;
        if record.layer_id != layer
            || record.state != LayerState::Deleting
            || record.schema_version != WORKSPACE_SCHEMA_VERSION
            || record.next_sequence == 0
            || encode(&record)? != *raw
        {
            return Err(WorkspaceError::Busy);
        }
        layer_inventory_generation(&values[2])?;
        let writes = if let Some(raw) = &values[3] {
            decode_state(raw, layer)?;
            Vec::new()
        } else {
            vec![put(
                state_key(layer),
                &State {
                    layer,
                    build: uuid::Uuid::new_v4(),
                    phase: Phase::Building {
                        after: None,
                        inventory: values[2].clone(),
                    },
                },
            )?]
        };
        let checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        budget.admit(&[]).map_err(|_| WorkspaceError::Busy)?;
        if !self.backend.compare_and_swap(&checks, &writes).await? {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }

    /// Explicit maintenance action. Requires protocol-aware writers throughout
    /// the deployment; runtime queries never start a hidden namespace scan.
    pub async fn start_native_reverse_index(
        &self,
        layer: LayerId,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<(), WorkspaceError> {
        self.require_admin_access()?;
        self.reverse_budget(&budget)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 1 << 20)])
            .map_err(|_| WorkspaceError::Busy)?;
        let keys = [
            hot_layer_key(layer),
            state_key(layer),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            CONTROL_KEY.to_vec(),
        ];
        let values = self.reverse_points(&keys).await?;
        validate_current_control_raw(values[3].as_deref())?;
        let record: LayerRecord = decode_open_value(
            values[0]
                .as_deref()
                .ok_or(WorkspaceError::LayerNotFound(layer))?,
            ROW_BYTES,
        )?;
        if record.layer_id != layer
            || record.schema_version != WORKSPACE_SCHEMA_VERSION
            || record.next_sequence == 0
            || record.state == LayerState::Deleting
        {
            return Err(WorkspaceError::Busy);
        }
        layer_inventory_generation(&values[2])?;
        if let Some(raw) = &values[1] {
            decode_state(raw, layer)?;
        }
        let state = State {
            layer,
            build: uuid::Uuid::new_v4(),
            phase: Phase::Building {
                after: None,
                inventory: values[2].clone(),
            },
        };
        let checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        budget.admit(&[]).map_err(|_| WorkspaceError::Busy)?;
        if !self
            .backend
            .compare_and_swap(&checks, &[put(state_key(layer), &state)?])
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }

    /// Persist at most one 16-row page. False means another call is required.
    /// An inventory change invalidates this build and requires an explicit new
    /// start; an old completion never grants a new layer incarnation authority.
    pub async fn advance_native_reverse_index(
        &self,
        layer: LayerId,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<bool, WorkspaceError> {
        self.require_admin_access()?;
        self.reverse_budget(&budget)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 1 << 20)])
            .map_err(|_| WorkspaceError::Busy)?;
        let keys = [
            hot_layer_key(layer),
            state_key(layer),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            CONTROL_KEY.to_vec(),
        ];
        let values = self.reverse_points(&keys).await?;
        validate_current_control_raw(values[3].as_deref())?;
        let record: LayerRecord = decode_open_value(
            values[0]
                .as_deref()
                .ok_or(WorkspaceError::LayerNotFound(layer))?,
            ROW_BYTES,
        )?;
        if record.layer_id != layer
            || record.schema_version != WORKSPACE_SCHEMA_VERSION
            || record.next_sequence == 0
            || record.state == LayerState::Deleting
        {
            return Err(WorkspaceError::Busy);
        }
        let mut state = decode_state(values[1].as_deref().ok_or(WorkspaceError::Busy)?, layer)?;
        let Phase::Building { after, inventory } = &state.phase else {
            return Ok(true);
        };
        if inventory != &values[2] {
            return Err(WorkspaceError::Busy);
        }
        let entries = self
            .backend
            .scan_prefix_page_with_byte_limits(
                &dentry_layer_prefix(layer),
                after.as_deref(),
                limits(BUILD_ROWS),
            )
            .await?;
        let mut previous = after.clone().unwrap_or_else(|| dentry_layer_prefix(layer));
        let mut checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let mut writes = Vec::new();
        for entry in &entries {
            if entry.key <= previous || !entry.key.starts_with(&dentry_layer_prefix(layer)) {
                return Err(corrupt("native reverse backfill progress"));
            }
            let row: DentryDelta = decode_open_value(&entry.value, ROW_BYTES)?;
            validate_row(&row, &entry.key, layer)?;
            if row.op == DentryOp::Put {
                writes.push(put(row_key(&state, &row)?, &row)?);
            }
            checks.push(KvCheck {
                key: entry.key.clone(),
                expected: Some(entry.value.clone()),
            });
            previous = entry.key.clone();
        }
        let ready = entries.is_empty();
        state.phase = if ready {
            Phase::Ready
        } else {
            Phase::Building {
                after: Some(previous),
                inventory: inventory.clone(),
            }
        };
        writes.push(put(state_key(layer), &state)?);
        crate::workspace::stores::kv_backend::validate_bounded_authentication_checks(
            &checks,
            limits(checks.len()),
        )?;
        budget.admit(&[]).map_err(|_| WorkspaceError::Busy)?;
        if !self.backend.compare_and_swap(&checks, &writes).await? {
            return Err(WorkspaceError::Busy);
        }
        Ok(ready)
    }

    pub(super) async fn native_reverse_authority(
        &self,
        layers: &[LayerRecord],
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<WorkspaceNativeReverseAuthority, WorkspaceError> {
        self.reverse_budget(&budget)?;
        if layers.len() != 2 || layers[0].layer_id == layers[1].layer_id {
            return Err(WorkspaceError::Busy);
        }
        let owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 1 << 20)])
            .map_err(|_| WorkspaceError::Busy)?;
        let mut keys = vec![LAYER_INVENTORY_GENERATION_KEY.to_vec()];
        for layer in layers {
            keys.extend([hot_layer_key(layer.layer_id), state_key(layer.layer_id)]);
        }
        let values = self.reverse_points(&keys).await?;
        layer_inventory_generation(&values[0])?;
        let mut builds = Vec::new();
        for (index, layer) in layers.iter().enumerate() {
            if values[1 + index * 2].as_deref() != Some(encode(layer)?.as_slice())
                || layer.state == LayerState::Deleting
            {
                return Err(WorkspaceError::Busy);
            }
            let state = decode_state(
                values[2 + index * 2]
                    .as_deref()
                    .ok_or(WorkspaceError::Busy)?,
                layer.layer_id,
            )?;
            if state.phase != Phase::Ready {
                return Err(WorkspaceError::Busy);
            }
            builds.push((state.layer, state.build));
        }
        Ok(WorkspaceNativeReverseAuthority {
            backend_identity: Arc::as_ptr(&self.backend) as usize,
            _backend_owner: self.backend.clone(),
            budget_identity: Arc::as_ptr(&budget) as usize,
            builds,
            checks: keys
                .into_iter()
                .zip(values)
                .map(|(key, expected)| KvCheck { key, expected })
                .collect(),
            _memory_guard: Arc::new(owner),
        })
    }

    fn validate_reverse_authority(
        &self,
        proof: &WorkspaceNativeReverseAuthority,
        budget: &Arc<SnapshotMountBudget>,
    ) -> Result<(), WorkspaceError> {
        self.reverse_budget(budget)?;
        if proof.backend_identity != Arc::as_ptr(&self.backend) as usize
            || proof.budget_identity != Arc::as_ptr(budget) as usize
            || proof.builds.len() != 2
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    pub(super) async fn native_reverse_page(
        &self,
        proof: &WorkspaceNativeReverseAuthority,
        layer: LayerId,
        ino: i64,
        after: Option<(i64, &[u8])>,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<WorkspaceNamePage<DentryDelta>, WorkspaceError> {
        self.validate_reverse_authority(proof, &budget)?;
        if ino <= 0 || after.is_some_and(|(parent, name)| parent <= 0 || !valid_name(name)) {
            return Err(WorkspaceError::InvalidReadPlan(
                "native reverse page cursor".into(),
            ));
        }
        let build = proof
            .builds
            .iter()
            .find(|(id, _)| *id == layer)
            .map(|(_, build)| *build)
            .ok_or(WorkspaceError::Fenced)?;
        let state = State {
            layer,
            build,
            phase: Phase::Ready,
        };
        let prefix = inode_prefix(layer, build, ino);
        let cursor = after.map(|(parent, name)| {
            let mut key = prefix.clone();
            key.extend_from_slice(
                format!("{}/{}", ino_component(parent), hex::encode(name)).as_bytes(),
            );
            key
        });
        let page = self
            .bounded_name_page(
                prefix,
                cursor,
                limits(POINT_ROWS),
                budget,
                move |row: &DentryDelta| {
                    if row.ino != Some(ino) {
                        return Err(corrupt("native reverse inode mismatch"));
                    }
                    row_key(&state, row)
                },
            )
            .await?;
        // A persisted reverse value must still match its authoritative forward
        // row. The fixed layer fence rejects any writer between these reads.
        if !page.rows.is_empty() {
            let keys = page.rows.iter().map(dentry_key).collect::<Vec<_>>();
            let values = self.reverse_points(&keys).await?;
            for (row, raw) in page.rows.iter().zip(values) {
                if raw.as_deref() != Some(encode(row)?.as_slice()) {
                    return Err(corrupt("stale native reverse row"));
                }
            }
        }
        Ok(page)
    }

    pub(super) async fn confirm_native_reverse(
        &self,
        proof: &WorkspaceNativeReverseAuthority,
        budget: Arc<SnapshotMountBudget>,
    ) -> Result<(), WorkspaceError> {
        self.validate_reverse_authority(proof, &budget)?;
        let _owner = budget
            .admit(&[(SnapshotBudgetPool::Metadata, 256 << 10)])
            .map_err(|_| WorkspaceError::Busy)?;
        if !self
            .backend
            .authenticate_checks_before_bounded(&proof.checks, i64::MAX, limits(proof.checks.len()))
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }
}
