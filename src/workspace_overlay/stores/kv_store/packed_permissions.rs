//! Packed permission reads and metadata copy-up in the actual native KV CAS.

use super::super::kv_backend::KvReadLimits;
use super::*;
use crate::meta::posix_acl::{ACCESS_XATTR, DEFAULT_XATTR};
use crate::workspace_overlay::packed_v3::wire005::{V3BudgetPool, V3MountBudget};

const VALUE_BYTES: usize = 96 << 10;
const READ_BYTES: usize = 97 << 10;
const READ_OPERATION_BYTES: u64 = 1 << 20;
const OPERATION_BYTES: u64 = 32 << 20;
const WRITE_BYTES: usize = 8 << 20;
const MAX_ROWS: usize = 32768;
const PERMISSION_NAMES: [&[u8]; 3] = [ACCESS_XATTR, DEFAULT_XATTR, b"system.brewfs.acl"];

struct PackedPermissionView {
    layers: [LayerRecord; 2],
    lease: SnapshotLease,
    checks: Vec<KvCheck>,
}

/// The exact post-commit bytes of a packed writer's view keys, as written or
/// checked by its own last successful commit. The mount is the sole writer
/// (single active lease, no seal, head never rotates under it), so these are
/// the store's values unless something outside the mount changed them: lease
/// renewal, an admin fence or GC. Using them as the next commit's CAS
/// conditions only removes the pre-read; every condition is still compared
/// atomically and the live-lease deadline is still enforced at the CAS, so a
/// stale entry can only fail the CAS (then the commit re-reads), never commit
/// against state the mount has not validated.
#[derive(Clone)]
pub(super) struct CommitViewCache {
    guard: HeadGuard,
    binding: PackedLowerBinding,
    /// `view_keys(..)` + CONTROL, in order.
    checks: Vec<KvCheck>,
    layers: [LayerRecord; 2],
    lease_expires_at_ns: i64,
    /// Post-commit bytes of the derived keys this writer re-reads every
    /// commit (head-layer reverse state, extent generation).
    derived: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl CommitViewCache {
    /// Prefetched points for this request's derived preparation: cached
    /// derived bytes, and absence for its fresh slices' deletion keys. Every
    /// one becomes an exact CAS check, so a wrong guess only fails the CAS.
    fn derived_points(
        &self,
        request: &VersionedMutation,
    ) -> std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        let mut points = std::collections::BTreeMap::new();
        for key in derived_prefetch_keys(request) {
            if let Some(value) = self.derived.get(&key) {
                points.insert(key, value.clone());
            } else if key.starts_with(SLICE_DELETION_KEY_PREFIX) {
                points.insert(key, None);
            }
        }
        points
    }

    fn matches(&self, guard: &HeadGuard, binding: &PackedLowerBinding) -> bool {
        &self.guard == guard && &self.binding == binding
    }

    /// Advance the cache across one committed packet: keys it wrote take
    /// their new bytes; the head record is the committed one.
    fn after_commit(mut self, writes: &[KvWrite], head: &LayerRecord) -> Option<Self> {
        for check in &mut self.checks {
            if let Some(write) = writes.iter().find(|write| match write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => *key == check.key,
            }) {
                check.expected = match write {
                    KvWrite::Put { value, .. } => Some(value.clone()),
                    KvWrite::Delete { .. } => None,
                };
            }
        }
        if self.layers[0].layer_id != head.layer_id {
            return None;
        }
        for (key, value) in &mut self.derived {
            if let Some(write) = writes.iter().find(|write| match write {
                KvWrite::Put { key: written, .. } | KvWrite::Delete { key: written } => {
                    written == key
                }
            }) {
                *value = match write {
                    KvWrite::Put { value, .. } => Some(value.clone()),
                    KvWrite::Delete { .. } => None,
                };
            }
        }
        self.layers[0] = head.clone();
        Some(self)
    }
}

const SLICE_DELETION_KEY_PREFIX: &[u8] = b"packed/v3/native-slice-delete/";

/// `view_keys` positions a cached (sole-writer) commit still compares: the
/// workspace, head, lease, writer-authority and open records, plus CONTROL
/// (a schema migration needs no lease). The lower layer, PWB current/claim/
/// history and recovery rows change only through paths that must hold this
/// workspace's single live lease; while the lease row is byte-identical and
/// live (both checked here, the deadline at the CAS), none of them ran. Any
/// change to a kept row fails the CAS and the retry re-reads and checks all.
const CACHED_VIEW_CHECKS: [usize; 6] = [0, 1, 3, 8, 9, 11];

/// Derived keys whose final checked bytes the next commit may reuse.
fn cacheable_derived_key(key: &[u8], head: LayerId) -> bool {
    key == super::native_slice_deletion::layer_extent_generation_key(head).as_slice()
        || key == super::native_reverse::state_key(head).as_slice()
}

fn limits(records: usize) -> KvReadLimits {
    KvReadLimits {
        max_records: records,
        max_key_bytes: 1024,
        max_value_bytes: VALUE_BYTES,
        max_total_bytes: READ_BYTES,
        max_response_bytes: 128 << 10,
        max_data_requests: records,
    }
}

fn view_keys(
    guard: &HeadGuard,
    binding: &PackedLowerBinding,
) -> Result<Vec<Vec<u8>>, WorkspaceError> {
    if binding.binding_version == 0 || binding.base_layer_id == guard.expected_head_layer_id {
        return Err(WorkspaceError::Fenced);
    }
    Ok(vec![
        hot_workspace_key(guard.workspace_id),
        hot_layer_key(guard.expected_head_layer_id),
        hot_layer_key(binding.base_layer_id),
        hot_lease_key(guard.workspace_id, guard.lease_id),
        packed_current_key(guard.workspace_id),
        packed_claim_key(guard.workspace_id),
        packed_history_key(guard.workspace_id, binding.binding_version),
        packed_history_key(guard.workspace_id, 1),
        packed_writer_authority::packed_writer_key(guard.workspace_id),
        open_v3_key(guard.workspace_id),
        open_v3_recovery_key(guard.workspace_id),
    ])
}

fn decode_view(
    keys: &[Vec<u8>],
    values: &[Option<Vec<u8>>],
    now: i64,
    guard: &HeadGuard,
    binding: &PackedLowerBinding,
    backend_identity: usize,
    budget: Option<&Arc<V3MountBudget>>,
) -> Result<PackedPermissionView, WorkspaceError> {
    if values.len() != keys.len() || values.len() < 11 {
        return Err(WorkspaceError::CorruptMetadata(
            "short packed permission view".into(),
        ));
    }
    let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
    let workspace: WorkspaceRecord = decode_open_value(required(0)?, VALUE_BYTES)?;
    let head: LayerRecord = decode_open_value(required(1)?, VALUE_BYTES)?;
    let base: LayerRecord = decode_open_value(required(2)?, VALUE_BYTES)?;
    let lease: SnapshotLease = decode_open_value(required(3)?, VALUE_BYTES)?;
    checked_hot_guard(&workspace, &head, &lease, guard, now)?;
    let layers = [head, base];
    validate_permission_layers(&layers)?;
    let record = decode_packed_pair(guard.workspace_id, &values[4], &values[5], &values[6])?
        .ok_or(WorkspaceError::Fenced)?;
    record.validate_for_guard(guard, &layers[1])?;
    if &record.binding != binding {
        return Err(WorkspaceError::Fenced);
    }
    let anchor = PackedLowerBindingRecord::decode(required(7)?)?;
    if anchor.workspace_id != guard.workspace_id || anchor.binding.binding_version != 1 {
        return Err(WorkspaceError::CorruptMetadata(
            "packed permission history anchor".into(),
        ));
    }
    let checks = keys
        .iter()
        .cloned()
        .zip(values.iter().cloned())
        .map(|(key, expected)| KvCheck { key, expected })
        .collect::<Vec<_>>();
    if lease.base_revision != record.base_revision {
        return Err(WorkspaceError::Fenced);
    }
    packed_writer_fences::authenticate_ordinary_packed_writer(
        backend_identity,
        budget,
        &checks,
        guard,
        &lease,
        now,
    )?;
    Ok(PackedPermissionView {
        layers,
        lease,
        checks,
    })
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    fn packed_permission_budget(&self) -> Result<Arc<V3MountBudget>, WorkspaceError> {
        self.packed_reader_pin_budget
            .get()
            .cloned()
            .ok_or(WorkspaceError::UnsupportedCapability(
                "packed permission byte budget",
            ))
    }

    pub(super) async fn read_packed_permission_snapshot_bounded(
        &self,
        guard: HeadGuard,
        binding: PackedLowerBinding,
        request: PermissionSnapshotQuery,
    ) -> Result<PermissionSnapshot, WorkspaceError> {
        if !self.backend.supports_consistent_reads() {
            return Err(WorkspaceError::UnsupportedCapability(
                "atomic packed permission snapshot",
            ));
        }
        if request.inodes.is_empty()
            || request.inodes.len() > 2
            || request.inodes.iter().any(|ino| *ino <= 0)
            || (request.inodes.len() == 2 && request.inodes[0] == request.inodes[1])
            || request.layer_ids != [guard.expected_head_layer_id, binding.base_layer_id]
        {
            return Err(WorkspaceError::CorruptMetadata(
                "invalid packed permission query".into(),
            ));
        }
        if let Some((parent, name)) = &request.dentry {
            if *parent <= 0 || name.len() > 255 {
                return Err(WorkspaceError::CorruptMetadata(
                    "packed permission name limit".into(),
                ));
            }
            DentryDelta::put(request.layer_ids[0], *parent, name.clone(), 1, 0, 0).validate()?;
        }
        let _permit = self
            .packed_permission_budget()?
            .admit(&[(V3BudgetPool::Metadata, READ_OPERATION_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let mut keys = view_keys(&guard, &binding)?;
        for ino in &request.inodes {
            for layer in request.layer_ids {
                keys.push(inode_identity_key(layer, *ino));
                for name in PERMISSION_NAMES {
                    keys.push(xattr_identity_key(layer, *ino, name));
                }
            }
        }
        if let Some((parent, name)) = &request.dentry {
            for layer in request.layer_ids {
                keys.push(dentry_identity_key(layer, *parent, name));
            }
        }
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, limits(keys.len()))
            .await?;
        let view = decode_view(
            &keys,
            &values,
            now,
            &guard,
            &binding,
            Arc::as_ptr(&self.backend) as usize,
            self.packed_reader_pin_budget.get(),
        )?;
        // Expiry and the exact native/PWB snapshot are checked at a backend
        // locked validation point before these rows enter a policy decision.
        if !self
            .backend
            .compare_and_swap_before(&view.checks, &[], view.lease.expires_at_ns)
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        let mut snapshot = PermissionSnapshot {
            layers: view.layers,
            inodes: Vec::new(),
            xattrs: Vec::new(),
            dentries: Vec::new(),
        };
        let mut index = 11;
        for ino in request.inodes {
            for layer in request.layer_ids {
                if let Some(value) = &values[index] {
                    let row: InodeDelta = decode_open_value(value, VALUE_BYTES)?;
                    if row.layer_id != layer || row.ino != ino {
                        return Err(WorkspaceError::CorruptMetadata(
                            "packed permission inode identity".into(),
                        ));
                    }
                    snapshot.inodes.push(row);
                }
                index += 1;
                for name in PERMISSION_NAMES {
                    if let Some(value) = &values[index] {
                        let row: XattrDelta = decode_open_value(value, VALUE_BYTES)?;
                        if row.layer_id != layer || row.ino != ino || row.name != name {
                            return Err(WorkspaceError::CorruptMetadata(
                                "packed permission xattr identity".into(),
                            ));
                        }
                        validate_value(row.op, row.value.as_deref(), "packed permission xattr")?;
                        if row.value.as_ref().is_some_and(|value| value.len() > 65536) {
                            return Err(WorkspaceError::CorruptMetadata(
                                "packed permission xattr value limit".into(),
                            ));
                        }
                        snapshot.xattrs.push(row);
                    }
                    index += 1;
                }
            }
        }
        if let Some((parent, name)) = request.dentry {
            for layer in request.layer_ids {
                if let Some(value) = &values[index] {
                    let row: DentryDelta = decode_open_value(value, VALUE_BYTES)?;
                    if row.layer_id != layer || row.parent_ino != parent || row.name != name {
                        return Err(WorkspaceError::CorruptMetadata(
                            "packed permission dentry identity".into(),
                        ));
                    }
                    row.validate()?;
                    snapshot.dentries.push(row);
                }
                index += 1;
            }
        }
        Ok(snapshot)
    }

    pub(super) async fn apply_packed_versioned_mutation_owned(
        &self,
        request: VersionedMutation,
        binding: PackedLowerBinding,
    ) -> Result<MutationResult, WorkspaceError> {
        if !self.backend.supports_consistent_reads() {
            return Err(WorkspaceError::UnsupportedCapability(
                "conditional packed metadata commit",
            ));
        }
        let count = [
            request.dentries.len(),
            request.inodes.len(),
            request.xattrs.len(),
            request.acls.len(),
            request.extents.len(),
        ]
        .into_iter()
        .try_fold(0usize, |count, rows| count.checked_add(rows))
        .ok_or_else(|| {
            WorkspaceError::InvalidReadPlan("packed mutation row count overflow".into())
        })?;
        if count > MAX_ROWS
            || request.dentries.iter().any(|row| row.name.len() > 255)
            || request.inodes.iter().any(|row| {
                row.symlink_target
                    .as_ref()
                    .is_some_and(|value| value.len() > 4096)
            })
            || request.xattrs.iter().any(|row| {
                row.name.len() > 255 || row.value.as_ref().is_some_and(|value| value.len() > 65536)
            })
            || request
                .acls
                .iter()
                .any(|row| row.value.as_ref().is_some_and(|value| value.len() > 65536))
        {
            return Err(WorkspaceError::InvalidReadPlan(
                "packed mutation row/value limit".into(),
            ));
        }
        let budget = self.packed_permission_budget()?;
        // Refuse oversized owned templates before validation clones keys or
        // row encoding allocates a write list. Include caller Vec capacities.
        let bound = validate_mutation_allocation_bounds(&request)?;
        let permit = budget
            .admit(&[(V3BudgetPool::Metadata, bound)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        if request.validate()? != count {
            return Err(WorkspaceError::Fenced);
        }
        let backend = self.backend.clone();
        let committed_head = self.committed_head.clone();
        let commit_view = self.commit_view.clone();
        let mutation_gate = self.packed_mutation_gate.clone();
        let recovery_owner = packed_admin::capture_mounted_recovery_owner();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let _mutation_guard = mutation_gate.lock().await;
            let mut store =
                KvWorkspaceStore::from_arc(backend).with_packed_reader_pin_budget(budget);
            store.commit_view = commit_view;
            let commit = store.commit_packed_versioned_mutation(request, binding, count);
            let commit = async move {
                commit.await.map(|(result, head)| {
                    *committed_head.lock().unwrap() = Some(head);
                    result
                })
            };
            let result = match recovery_owner {
                Some(owner) => owner.scope(commit).await,
                None => commit.await,
            };
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|error| WorkspaceError::Backend(format!("packed mutation driver: {error}")))?
    }

    async fn commit_packed_versioned_mutation(
        &self,
        request: VersionedMutation,
        binding: PackedLowerBinding,
        count: usize,
    ) -> Result<(MutationResult, LayerRecord), WorkspaceError> {
        let mut keys = view_keys(&request.guard, &binding)?;
        keys.push(CONTROL_KEY.to_vec());
        let view_len = keys.len();
        // Keys the derived native-reverse/extent preparation will read, known
        // from the request rows alone. Reading them in this same consistent
        // view read removes their separate round trips; every one is still a
        // check in the final CAS, so a concurrent change fails that CAS.
        keys.extend(derived_prefetch_keys(&request));
        // Recovery-scoped writers authenticate a token against fresh reads;
        // they never use the cached view.
        let cacheable = packed_admin::capture_mounted_recovery_owner().is_none();
        for _ in 0..CAS_MAX_RETRIES {
            let cached = if cacheable {
                self.commit_view
                    .lock()
                    .unwrap()
                    .take()
                    .filter(|cache| cache.matches(&request.guard, &binding))
            } else {
                None
            };
            let from_cache = cached.is_some();
            // Derived bytes this writer committed earlier (create commits
            // carry reverse state, extent commits the extent generation);
            // keep them across commits of the other kind.
            let mut carried = cached
                .as_ref()
                .map(|cache| cache.derived.clone())
                .unwrap_or_default();
            let (view, prefetched) = match cached {
                // No pre-read: the cached post-commit bytes are the CAS
                // conditions, including the derived points.
                Some(cache) => {
                    let points = cache.derived_points(&request);
                    (
                        PackedPermissionView {
                            layers: cache.layers.clone(),
                            lease: SnapshotLease {
                                expires_at_ns: cache.lease_expires_at_ns,
                                ..decode_open_value(
                                    cache.checks[3]
                                        .expected
                                        .as_deref()
                                        .ok_or(WorkspaceError::Fenced)?,
                                    VALUE_BYTES,
                                )?
                            },
                            checks: cache.checks,
                        },
                        Some(points),
                    )
                }
                None => {
                    let (all_values, now) = self
                        .backend
                        .get_many_consistent_with_time_bounded(&keys, limits(keys.len()))
                        .await?;
                    if all_values.len() != keys.len() {
                        return Err(WorkspaceError::CorruptMetadata(
                            "short packed mutation view read".into(),
                        ));
                    }
                    let prefetched: std::collections::BTreeMap<_, _> = keys[view_len..]
                        .iter()
                        .cloned()
                        .zip(all_values[view_len..].iter().cloned())
                        .collect();
                    let values = &all_values[..view_len];
                    let keys_view = &keys[..view_len];
                    validate_current_control_raw(values.last().and_then(Option::as_deref))?;
                    let view = decode_view(
                        keys_view,
                        values,
                        now,
                        &request.guard,
                        &binding,
                        Arc::as_ptr(&self.backend) as usize,
                        self.packed_reader_pin_budget.get(),
                    )?;
                    (view, Some(prefetched))
                }
            };
            if view.layers != request.expected_layers {
                if from_cache {
                    // The caller's expected head disagrees with the cache;
                    // let the fresh read decide.
                    continue;
                }
                return Err(WorkspaceError::Busy);
            }
            let mut head = view.layers[0].clone();
            let mut writes = Vec::new();
            let result = append_versioned_rows(&request, &mut head, count, &mut writes)?;
            let _extent_compaction = self
                .compact_extent_history(
                    &head,
                    Some(&view.layers[1]),
                    MAX_UPPER_EXTENT_ROWS,
                    &mut writes,
                )
                .await?;
            if count != 0 {
                writes.push(put(keys[1].clone(), &head)?);
            }
            let view_checks = view.checks.clone();
            let mut checks = if from_cache {
                CACHED_VIEW_CHECKS
                    .iter()
                    .map(|index| view.checks[*index].clone())
                    .collect()
            } else {
                view.checks
            };
            let derived = async {
                let reverse = self
                    .prepare_native_reverse_cas(&mut checks, &mut writes)
                    .await?;
                let extent = self
                    .prepare_native_extent_cas(&mut checks, &mut writes)
                    .await?;
                Ok::<_, WorkspaceError>((reverse, extent))
            };
            let derived = match prefetched {
                Some(prefetched) => super::with_prefetched_points(prefetched, derived).await,
                None => derived.await,
            };
            let (_native_reverse, _extent_fence) = match derived {
                Ok(owners) => owners,
                // A cached guess the derived proof rejects: re-read.
                Err(WorkspaceError::Busy) if from_cache => continue,
                Err(error) => return Err(error),
            };
            carried.retain(|key, _| cacheable_derived_key(key, head.layer_id));
            carried.extend(
                checks
                    .iter()
                    .filter(|check| cacheable_derived_key(&check.key, head.layer_id))
                    .map(|check| (check.key.clone(), check.expected.clone())),
            );
            let derived_checks = carried;
            // All primary and derived entries use the original packet budget.
            if checks
                .len()
                .checked_add(writes.len())
                .is_none_or(|items| items > MAX_ROWS)
            {
                return Err(WorkspaceError::InvalidReadPlan(
                    "packed mutation packet item limit".into(),
                ));
            }
            let total = writes.iter().try_fold(0usize, |total, write| {
                let (key, value_bytes) = match write {
                    KvWrite::Put { key, value } => (key, value.len()),
                    KvWrite::Delete { key } => (key, 0),
                };
                if key.len() > 1024 || value_bytes > VALUE_BYTES {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "packed mutation stored value limit".into(),
                    ));
                }
                total
                    .checked_add(key.len())
                    .and_then(|total| total.checked_add(value_bytes))
                    .ok_or_else(|| {
                        WorkspaceError::InvalidReadPlan("packed mutation bytes overflow".into())
                    })
            })?;
            let total = checks.iter().try_fold(total, |total, check| {
                total
                    .checked_add(check.key.len())
                    .and_then(|total| {
                        total.checked_add(check.expected.as_ref().map_or(0, Vec::len))
                    })
                    .ok_or_else(|| {
                        WorkspaceError::InvalidReadPlan(
                            "packed mutation check bytes overflow".into(),
                        )
                    })
            })?;
            if total > WRITE_BYTES {
                return Err(WorkspaceError::InvalidReadPlan(
                    "packed mutation aggregate write limit".into(),
                ));
            }
            // Exact PWB current/claim/history/anchor and the native view share
            // the actual row/sequence CAS and its live-lease deadline. Backend
            // uncertainty is returned, never blindly replayed or rolled back.
            let committed = match self
                .backend
                .compare_and_swap_before(&checks, &writes, view.lease.expires_at_ns)
                .await
            {
                Ok(committed) => committed,
                // A cached lease deadline may simply be older than a renewal
                // the mount already made; re-read before reporting a fence.
                Err(WorkspaceError::Fenced) if from_cache => false,
                Err(error) => return Err(error),
            };
            if committed {
                if cacheable {
                    *self.commit_view.lock().unwrap() = CommitViewCache {
                        guard: request.guard.clone(),
                        binding: binding.clone(),
                        checks: view_checks,
                        layers: view.layers.clone(),
                        lease_expires_at_ns: view.lease.expires_at_ns,
                        derived: derived_checks,
                    }
                    .after_commit(&writes, &head);
                }
                // `head` is exactly the committed head record.
                return Ok((result, head));
            }
            if !from_cache {
                tokio::task::yield_now().await;
            }
        }
        Err(WorkspaceError::Busy)
    }
}

/// Returns the owned-plus-stored bytes this mutation can retain, which the
/// commit admits instead of a fixed worst-case arena.
/// Exact derived keys the native-reverse and extent preparation read for
/// this request. Bounded so the combined view read stays within its byte and
/// record limits; a request over the bound just reads them separately.
fn derived_prefetch_keys(request: &VersionedMutation) -> Vec<Vec<u8>> {
    const MAX_PREFETCHED_SLICES: usize = 32;
    let mut keys = Vec::new();
    if !request.dentries.is_empty() {
        keys.push(super::native_reverse::state_key(
            request.guard.expected_head_layer_id,
        ));
    }
    let slices: std::collections::BTreeSet<u64> = request
        .extents
        .iter()
        .filter_map(|row| match row.kind {
            ExtentKind::Data { slice_id, .. } => Some(slice_id),
            ExtentKind::Hole => None,
        })
        .collect();
    if !request.extents.is_empty() && slices.len() <= MAX_PREFETCHED_SLICES {
        keys.push(super::native_slice_deletion::layer_extent_generation_key(
            request.guard.expected_head_layer_id,
        ));
        keys.extend(
            slices
                .into_iter()
                .map(super::native_slice_deletion::slice_deletion_key),
        );
    }
    keys
}

fn validate_mutation_allocation_bounds(request: &VersionedMutation) -> Result<u64, WorkspaceError> {
    fn overflow() -> WorkspaceError {
        WorkspaceError::InvalidReadPlan("packed mutation allocation limit".into())
    }
    fn vector_bytes<T>(rows: &Vec<T>) -> Result<usize, WorkspaceError> {
        rows.capacity()
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(overflow)
    }
    fn add(total: &mut usize, bytes: usize) -> Result<(), WorkspaceError> {
        *total = total.checked_add(bytes).ok_or_else(overflow)?;
        if *total > WRITE_BYTES {
            return Err(overflow());
        }
        Ok(())
    }
    fn encoded<T: Serialize>(
        total: &mut usize,
        row: &T,
        key: Vec<u8>,
    ) -> Result<(), WorkspaceError> {
        let payload = usize::try_from(bincode::serialized_size(row).map_err(|error| {
            WorkspaceError::Backend(format!("size packed mutation row: {error}"))
        })?)
        .map_err(|_| overflow())?;
        let value = payload
            .checked_add(ENVELOPE_MAGIC.len())
            .ok_or_else(overflow)?;
        if value > VALUE_BYTES || key.len() > 1024 {
            return Err(overflow());
        }
        add(total, value.checked_add(key.len()).ok_or_else(overflow)?)
    }
    let mut owned = std::mem::size_of::<VersionedMutation>();
    for bytes in [
        vector_bytes(&request.dentries)?,
        vector_bytes(&request.inodes)?,
        vector_bytes(&request.xattrs)?,
        vector_bytes(&request.acls)?,
        vector_bytes(&request.extents)?,
    ] {
        add(&mut owned, bytes)?;
    }
    // Reserve one maximum fixed view/head envelope as well as all row bytes.
    let mut stored = READ_BYTES;
    for row in &request.dentries {
        add(&mut owned, row.name.capacity())?;
        encoded(&mut stored, row, dentry_key(row))?;
    }
    for row in &request.inodes {
        add(
            &mut owned,
            row.symlink_target.as_ref().map_or(0, Vec::capacity),
        )?;
        encoded(&mut stored, row, inode_key(row))?;
    }
    for row in &request.xattrs {
        add(&mut owned, row.name.capacity())?;
        add(&mut owned, row.value.as_ref().map_or(0, Vec::capacity))?;
        encoded(&mut stored, row, xattr_key(row))?;
    }
    for row in &request.acls {
        add(&mut owned, row.value.as_ref().map_or(0, Vec::capacity))?;
        encoded(&mut stored, row, acl_key(row))?;
    }
    for row in &request.extents {
        encoded(&mut stored, row, extent_key(row))?;
    }
    // Checks/derived native reverse and extent CAS entries are at most a
    // fixed view packet plus one derived pair per row; bound them by doubling
    // the stored bytes and adding the fixed view envelope.
    let bound = owned
        .checked_add(stored.checked_mul(2).ok_or_else(overflow)?)
        .and_then(|bytes| bytes.checked_add(1 << 20))
        .ok_or_else(overflow)?;
    Ok(u64::try_from(bound)
        .map_err(|_| overflow())?
        .min(OPERATION_BYTES))
}

fn append_versioned_rows(
    request: &VersionedMutation,
    head: &mut LayerRecord,
    count: usize,
    writes: &mut Vec<KvWrite>,
) -> Result<MutationResult, WorkspaceError> {
    let Some((first, last)) = allocate_layer_sequences(head, count)? else {
        return Ok(MutationResult {
            first_sequence: None,
            last_sequence: None,
        });
    };
    let mut sequence = first;
    for template in &request.dentries {
        let mut row = template.clone();
        row.sequence = sequence;
        sequence += 1;
        writes.push(put(dentry_key(&row), &row)?);
    }
    for template in &request.xattrs {
        let mut row = template.clone();
        row.sequence = sequence;
        sequence += 1;
        writes.push(put(xattr_key(&row), &row)?);
    }
    for template in &request.acls {
        let mut row = template.clone();
        row.sequence = sequence;
        sequence += 1;
        writes.push(put(acl_key(&row), &row)?);
    }
    for template in &request.inodes {
        let mut row = template.clone();
        row.sequence = sequence;
        sequence += 1;
        writes.push(put(inode_key(&row), &row)?);
    }
    for template in &request.extents {
        let mut row = template.clone();
        row.sequence = sequence;
        sequence += 1;
        if matches!(row.kind, ExtentKind::Data { .. }) {
            head.owned_slice_count = head.owned_slice_count.checked_add(1).ok_or_else(|| {
                WorkspaceError::CorruptMetadata("owned slice count overflow".into())
            })?;
            head.owned_bytes = head.owned_bytes.checked_add(row.length).ok_or_else(|| {
                WorkspaceError::CorruptMetadata("owned byte count overflow".into())
            })?;
        }
        writes.push(put(extent_key(&row), &row)?);
    }
    Ok(MutationResult {
        first_sequence: Some(first),
        last_sequence: Some(last),
    })
}
