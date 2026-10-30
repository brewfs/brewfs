//! A fork borrows one exact retained graph without duplicating its RootRow.
//! Alias retirement deletes child metadata only; object memberships stay owned
//! by the source graph and its original cross-workspace ancestry census.

use super::collector::{GATE_KEY, require_active_retirement_gate};
use super::*;
use crate::workspace_overlay::stores::kv_store::packed_carrier_basis::{
    PackedCarrierBasis, packed_carrier_basis_key, packed_carrier_claim_key,
};
use crate::workspace_overlay::stores::kv_store::packed_writer_authority::{
    PackedWriterAuthority, packed_writer_key,
};

const ALIAS_MAX_BYTES: usize = 12 << 10;
const ALIAS_OPERATION_BYTES: u64 = 4 << 20;
const ALIAS_RETIREMENT_BYTES: u64 = 8 << 20;
const ALIAS_PREFIX: &[u8] = b"packed/v3/borrowed-history/";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BorrowedHistoryAlias {
    binding: PackedLowerBindingRecord,
    carrier_basis_digest: [u8; 32],
    source_incarnation: Uuid,
}

pub(crate) fn borrowed_history_key(binding: &PackedLowerBindingRecord) -> Vec<u8> {
    format!(
        "packed/v3/borrowed-history/{}/{:016x}",
        binding.workspace_id, binding.binding.binding_version
    )
    .into_bytes()
}

fn retirement_key(binding: &PackedLowerBindingRecord) -> Vec<u8> {
    format!(
        "packed/v3/borrowed-retirement/{}/{:016x}",
        binding.workspace_id, binding.binding.binding_version
    )
    .into_bytes()
}

impl BorrowedHistoryAlias {
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        if self.binding.binding.binding_version != 1
            || self.binding.head_epoch != 1
            || self.source_incarnation.is_nil()
            || self.carrier_basis_digest == [0; 32]
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut bytes = row_header(b"PBA3");
        append_bytes(&mut bytes, &self.binding.encode()?, REFERENCE_LIMIT)?;
        bytes.extend_from_slice(&self.carrier_basis_digest);
        bytes.extend_from_slice(self.source_incarnation.as_bytes());
        finish_record(bytes, ALIAS_MAX_BYTES)
    }
    pub(super) fn decode(raw: &[u8]) -> Result<Self, WorkspaceError> {
        let mut cursor = JournalCursor::checked(raw, b"PBA3", ALIAS_MAX_BYTES)?;
        let value = Self {
            binding: PackedLowerBindingRecord::decode(&cursor.bytes(REFERENCE_LIMIT)?)?,
            carrier_basis_digest: cursor.take()?,
            source_incarnation: Uuid::from_bytes(cursor.take()?),
        };
        cursor.end()?;
        value.encode()?;
        Ok(value)
    }
}

pub(crate) struct BorrowedWorkspacePlan {
    pub(crate) binding: Option<PackedLowerBindingRecord>,
    pub(crate) next_inode: Option<i64>,
    pub(crate) checks: Vec<KvCheck>,
    pub(crate) writes: Vec<KvWrite>,
    _permit: Option<V3OwnedPermit>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BorrowedHistoryRetirementReport {
    pub(crate) retired: bool,
    pub(crate) source_members_released: u64,
}

pub(super) struct BorrowedHistoryCensus {
    pub(super) checks: Vec<KvCheck>,
    pub(super) matched: u64,
    _permit: V3OwnedPermit,
}

pub(crate) struct CarrierMetadataDeletion {
    pub(crate) checks: Vec<KvCheck>,
    pub(crate) writes: Vec<KvWrite>,
    _permit: Option<V3OwnedPermit>,
}

fn is_carrier_deletion_check(key: &[u8]) -> bool {
    key.starts_with(b"packed/v3/sealed-carrier/")
        || key.starts_with(b"packed/v3/carrier-claim/")
        || key.starts_with(b"packed/v3/registry/root/")
        || key.starts_with(b"packed/v3/registry/history-root/")
        || key.starts_with(b"packed/v3/history/")
}
// Binding history version 1 is the durable source installed by the initial
// bootstrap. A carrier may retain that source exactly as it retains a later
// publication version; only the zero version is invalid. Keep this check
// explicit so borrowed forks do not accidentally reject an initial source.
fn validate_retained_source_binding_version(version: u64) -> Result<(), WorkspaceError> {
    if version == 0 {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
}

fn merge(checks: &mut Vec<KvCheck>, added: Vec<KvCheck>) -> Result<(), WorkspaceError> {
    super::super::native_publication::append_exact_checks(checks, added)?;
    let bytes = checks.iter().try_fold(0usize, |bytes, check| {
        let value_bytes = check.expected.as_ref().map_or(0, Vec::len);
        if check.key.len() > 256 || value_bytes > RECORD_LIMIT {
            return Err(WorkspaceError::Fenced);
        }
        bytes
            .checked_add(check.key.len())
            .and_then(|bytes| bytes.checked_add(value_bytes))
            .ok_or(WorkspaceError::Busy)
    })?;
    if checks.len() > 512 || bytes > 1 << 20 {
        return Err(WorkspaceError::Busy);
    }
    Ok(())
}

fn exact_checks(keys: &[Vec<u8>], values: &[Option<Vec<u8>>]) -> Vec<KvCheck> {
    keys.iter()
        .cloned()
        .zip(values.iter().cloned())
        .map(|(key, expected)| KvCheck { key, expected })
        .collect()
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// Build the complete retained carrier proof for a caller-owned CAS.
    /// The caller must retain its canonical metadata permit until that CAS
    /// finishes. This read does not itself issue publication authority.
    pub(crate) async fn retained_packed_carrier_checks(
        &self,
        revision: &BaseRevision,
    ) -> Result<(PackedLowerBindingRecord, Vec<KvCheck>), WorkspaceError> {
        let keys = [
            packed_carrier_basis_key(revision.layer_id),
            packed_carrier_claim_key(revision.layer_id),
        ];
        let values = self.alias_values(&keys).await?;
        let basis = PackedCarrierBasis::decode_pair(revision, &values[0], &values[1])?
            .ok_or(WorkspaceError::Fenced)?;
        let mut checks = exact_checks(&keys, &values);
        merge(
            &mut checks,
            self.retained_carrier_source_checks(&basis).await?,
        )?;
        Ok((basis.source_binding, checks))
    }

    /// Inspect one mandatory packed carrier under its exact retained source.
    /// The full no-op CAS is required; decoded marker bytes alone grant nothing.
    pub async fn inspect_packed_carrier_revision(
        &self,
        revision: &BaseRevision,
    ) -> Result<PackedLowerBindingRecord, WorkspaceError> {
        let budget =
            self.packed_reader_pin_budget
                .get()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "canonical registry budget",
                ))?;
        let _owner = budget
            .admit(&[(V3BudgetPool::Metadata, ALIAS_OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        let (source_binding, checks) = self.retained_packed_carrier_checks(revision).await?;
        if !self.backend.compare_and_swap(&checks, &[]).await? {
            return Err(WorkspaceError::Busy);
        }
        Ok(source_binding)
    }

    pub(crate) fn is_carrier_deletion_check(&self, key: &[u8]) -> bool {
        is_carrier_deletion_check(key)
    }

    /// Descriptor and claim leave in the actual native layer deletion CAS,
    /// only after the original source graph reaches its retained terminal row.
    pub(crate) async fn prepare_carrier_metadata_deletion(
        &self,
        layer_ids: &[LayerId],
        layer_authorities: &[KvCheck],
    ) -> Result<CarrierMetadataDeletion, WorkspaceError> {
        let permit = self
            .packed_reader_pin_budget
            .get()
            .map(|budget| {
                budget
                    .admit(&[(V3BudgetPool::Metadata, ALIAS_OPERATION_BYTES)])
                    .map_err(journal_budget_error)
            })
            .transpose()?;
        let mut checks = Vec::new();
        let mut writes = Vec::new();
        for layer_id in layer_ids
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
        {
            let keys = [
                packed_carrier_basis_key(layer_id),
                packed_carrier_claim_key(layer_id),
            ];
            let values = self.alias_values(&keys).await?;
            if values.iter().all(Option::is_none) {
                continue;
            }
            if permit.is_none() {
                return Err(WorkspaceError::UnsupportedCapability(
                    "canonical registry budget",
                ));
            }
            let raw_layer = layer_authorities
                .iter()
                .find(|check| check.key == hot_layer_key(layer_id))
                .and_then(|check| check.expected.as_deref())
                .ok_or(WorkspaceError::Fenced)?;
            let layer: LayerRecord = decode_open_value(raw_layer, OPEN_RECORD_MAX_BYTES)?;
            if layer.layer_id != layer_id
                || layer.state != LayerState::Deleting
                || layer.parent_layer_id.is_some()
                || layer.depth != 1
            {
                return Err(WorkspaceError::Fenced);
            }
            let revision = BaseRevision {
                layer_id,
                sealed_version: layer.sealed_version.ok_or(WorkspaceError::Fenced)?,
                root_hash: layer.root_hash.ok_or(WorkspaceError::Fenced)?,
            };
            let basis = PackedCarrierBasis::decode_pair(&revision, &values[0], &values[1])?
                .ok_or(WorkspaceError::Fenced)?;
            let root_keys = [
                registry_root_key(basis.registry_incarnation),
                registry_history_root_key(&basis.source_binding),
                packed_history_key(
                    basis.source_binding.workspace_id,
                    basis.source_binding.binding.binding_version,
                ),
            ];
            let root_values = self.alias_values(&root_keys).await?;
            if root_values[0] != root_values[1] || root_values[2].is_some() {
                return Err(WorkspaceError::Fenced);
            }
            let root = RootRow::decode(root_values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
            if root.incarnation != basis.registry_incarnation
                || root.binding.as_ref() != Some(&basis.source_binding)
                || root.state != RootState::Retired
                || root.members != 0
                || root.pending_puts != 0
            {
                return Err(WorkspaceError::Busy);
            }
            merge(&mut checks, exact_checks(&keys, &values))?;
            merge(&mut checks, exact_checks(&root_keys, &root_values))?;
            writes.extend(keys.into_iter().map(|key| KvWrite::Delete { key }));
        }
        if !writes.is_empty() {
            // Deleting a packed carrier is a maintenance transition, like its
            // publication: keep the exact catalog header in the check set so
            // a concurrent migrate cannot interleave with the final CAS.
            // Ordinary layer GC without a carrier still excludes CONTROL.
            let control = [CONTROL_KEY.to_vec()];
            let values = self.alias_values(&control).await?;
            if values[0].is_none() {
                return Err(WorkspaceError::Fenced);
            }
            merge(&mut checks, exact_checks(&control, &values))?;
        }
        Ok(CarrierMetadataDeletion {
            checks,
            writes,
            _permit: permit,
        })
    }

    pub(crate) async fn create_workspace_with_carrier_expectation(
        &self,
        request: CreateWorkspace,
        require_packed_carrier: bool,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        let workspace_key = hot_workspace_key(request.workspace_id);
        let head_key = hot_layer_key(request.head_layer_id);
        for attempt in 0..CAS_MAX_RETRIES {
            let packed_fork = match self
                .prepare_borrowed_workspace(&request, require_packed_carrier)
                .await
            {
                Ok(plan) => plan,
                Err(WorkspaceError::Busy) => {
                    // The carrier probe is a bounded, pre-submit read. A
                    // remote KV lock can transiently reject that read, so
                    // rebuild the complete fork plan under this retry budget.
                    retry_backoff(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let mut scope = TopologyScope {
                workspaces: vec![request.workspace_id],
                layers: vec![request.base_revision.layer_id, request.head_layer_id],
                extra_keys: vec![LAYER_INVENTORY_GENERATION_KEY.to_vec()],
                ..TopologyScope::default()
            };
            scope
                .extra_keys
                .extend(packed_fork.checks.iter().map(|check| check.key.clone()));
            if packed_fork.next_inode.is_some() {
                scope.allocators.push("inode".into());
            }
            let basis = match self
                .read_topology_scope(&scope, topology_point_limits(32))
                .await
            {
                Ok(basis) => basis,
                Err(WorkspaceError::Busy) => {
                    // A concurrent fork may invalidate the bounded point
                    // snapshot before packet preparation. Re-read the full
                    // topology scope under the same finite retry budget.
                    retry_backoff(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let now = basis.now_ns;
            let inventory_generation = basis
                .checks
                .iter()
                .find(|check| check.key.as_slice() == LAYER_INVENTORY_GENERATION_KEY)
                .ok_or(WorkspaceError::Fenced)?
                .expected
                .clone();
            let next_inventory_generation = next_layer_inventory_generation(&inventory_generation)?;
            if basis.state.workspaces.contains_key(&request.workspace_id)
                || basis.state.layers.contains_key(&request.head_layer_id)
            {
                return Err(WorkspaceError::Busy);
            }
            let base = basis
                .state
                .layers
                .get(&request.base_revision.layer_id)
                .cloned()
                .ok_or(WorkspaceError::LayerNotFound(
                    request.base_revision.layer_id,
                ))?;
            if revision_from_layer(&base)? != request.base_revision {
                return Err(conflict("fork base revision changed"));
            }
            if base.parent_layer_id.is_some() || base.depth != 1 {
                return Err(WorkspaceError::CorruptMetadata(
                    "workspace base revision must be a flat sealed layer".into(),
                ));
            }
            let workspace = WorkspaceRecord {
                workspace_id: request.workspace_id,
                head_layer_id: request.head_layer_id,
                head_epoch: u64::from(packed_fork.binding.is_some()),
                fork_base: Some(request.base_revision.clone()),
                owner_id: request.owner_id.clone(),
                upper_bucket: request.upper_bucket.clone(),
                state: WorkspaceState::Active,
                active_lease: None,
                created_at_ns: now,
                updated_at_ns: now,
            };
            let head = writable_layer(
                request.head_layer_id,
                request.base_revision.layer_id,
                2,
                request.workspace_id,
                now,
            );
            let mut checks = basis.checks;
            // The catalog header is validated by packet preparation, but an
            // ordinary workspace fork does not modify it and must not retain
            // CONTROL as a cross-workspace CAS authority. Packed carrier
            // bootstrap remains an explicit maintenance path and keeps its
            // complete authority set.
            if !require_packed_carrier {
                checks.retain(|check| check.key.as_slice() != CONTROL_KEY);
            }
            let mut writes = vec![
                put(
                    LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                    &next_inventory_generation,
                )?,
                put(workspace_key.clone(), &workspace)?,
                put(head_key.clone(), &head)?,
            ];
            self.merge_borrowed_checks(&mut checks, packed_fork.checks.clone())?;
            writes.extend(packed_fork.writes.clone());
            let _writer_owner = if packed_fork.binding.is_some() {
                Some(
                    self.prepare_fork_packed_writer_authority(
                        request.workspace_id,
                        &mut checks,
                        &mut writes,
                    )
                    .await?,
                )
            } else {
                None
            };
            // Native reverse/owner preparation reads the same epoch keys that
            // the final topology packet updates. A concurrent fork can lose
            // that read-side overlap before the packet is submitted and is
            // reported as Busy by the preparation helpers. Treat only that
            // transient classification as another bounded fork attempt;
            // transport, corruption, budget, and semantic errors still
            // propagate unchanged.
            let _native_reverse = match self
                .prepare_native_reverse_cas(&mut checks, &mut writes)
                .await
            {
                Ok(owner) => owner,
                Err(WorkspaceError::Busy) => {
                    retry_backoff(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let _native_holds = match self
                .prepare_native_owner_cas(&mut checks, &mut writes)
                .await
            {
                Ok(owner) => owner,
                Err(WorkspaceError::Busy) => {
                    retry_backoff(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            // Native derived-index preparation can lose the shared topology
            // epoch while another fork commits.  The final envelope is still
            // a pre-submit operation, so rebuild the complete fork plan on
            // the same bounded retry budget instead of surfacing a transient
            // Busy to concurrent callers.
            let packet = match self.prepare_topology_envelope(checks, writes, None).await {
                Ok(packet) => packet,
                Err(WorkspaceError::Busy) => {
                    retry_backoff(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if self.commit_prepared_topology_packet(&packet).await? {
                return Ok(workspace);
            }
            // All concurrent forks authenticate the shared layer-inventory
            // generation. A scheduler yield alone keeps the thundering herd
            // synchronized and can exhaust CAS_MAX_RETRIES on remote KV
            // backends before any loser observes the committed generation.
            // Use the same bounded jittered backoff as other topology
            // mutations so Redis/TiKV contention converges without changing
            // the exact entity CAS contract.
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    /// Packed snapshot forks cannot fall back to an unmarked native revision.
    /// Each child uses the original birth CAS with mandatory carrier proof.
    pub async fn fork_packed_carrier_revision(
        &self,
        revision: BaseRevision,
        count: usize,
        owner_id: Option<String>,
    ) -> Result<Vec<WorkspaceRecord>, WorkspaceError> {
        self.require_admin_access()?;
        if count > 512 {
            return Err(WorkspaceError::Busy);
        }
        let mut workspaces = Vec::with_capacity(count);
        for _ in 0..count {
            workspaces.push(
                self.create_workspace_with_carrier_expectation(
                    CreateWorkspace {
                        workspace_id: WorkspaceId::new(),
                        head_layer_id: LayerId::new(),
                        base_revision: revision.clone(),
                        owner_id: owner_id.clone(),
                        upper_bucket: None,
                    },
                    true,
                )
                .await?,
            );
        }
        Ok(workspaces)
    }

    /// A deterministic child birth still requires the complete packed carrier.
    pub async fn create_workspace_from_packed_carrier(
        &self,
        request: CreateWorkspace,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        self.require_admin_access()?;
        self.create_workspace_with_carrier_expectation(request, true)
            .await
    }

    pub(crate) fn merge_borrowed_checks(
        &self,
        checks: &mut Vec<KvCheck>,
        added: Vec<KvCheck>,
    ) -> Result<(), WorkspaceError> {
        merge(checks, added)
    }
    // Two keys also bound corrupt 48KiB values below Redis's 128KiB wire cap.
    async fn alias_values(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
        let mut all = Vec::with_capacity(keys.len());
        for batch in keys.chunks(2) {
            let (values, now) = self
                .backend
                .get_many_consistent_with_time_bounded(
                    batch,
                    KvReadLimits {
                        max_records: 2,
                        max_data_requests: 2,
                        ..journal_point_limits()
                    },
                )
                .await?;
            if values.len() != batch.len() || now <= 0 {
                return Err(WorkspaceError::Fenced);
            }
            all.extend(values);
        }
        Ok(all)
    }

    pub(super) async fn borrowed_history_source_checks(
        &self,
        binding: &PackedLowerBindingRecord,
        alias: &BorrowedHistoryAlias,
    ) -> Result<Vec<KvCheck>, WorkspaceError> {
        if &alias.binding != binding {
            return Err(WorkspaceError::Fenced);
        }
        let keys = vec![
            packed_carrier_basis_key(binding.base_revision.layer_id),
            packed_carrier_claim_key(binding.base_revision.layer_id),
        ];
        let values = self.alias_values(&keys).await?;
        let basis =
            PackedCarrierBasis::decode_pair(&binding.base_revision, &values[0], &values[1])?
                .ok_or(WorkspaceError::Fenced)?;
        let descriptor_digest: [u8; 32] =
            Sha256::digest(values[0].as_ref().ok_or(WorkspaceError::Fenced)?).into();
        validate_retained_source_binding_version(basis.source_binding.binding.binding_version)?;
        if descriptor_digest != alias.carrier_basis_digest
            || basis.registry_incarnation != alias.source_incarnation
            || basis.source_binding.workspace_id == binding.workspace_id
            || basis.source_binding.binding.manifest != binding.binding.manifest
            || basis.source_binding.highest_inode != binding.highest_inode
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut checks = exact_checks(&keys, &values);
        merge(
            &mut checks,
            self.retained_carrier_source_checks(&basis).await?,
        )?;
        Ok(checks)
    }

    async fn retained_carrier_source_checks(
        &self,
        basis: &PackedCarrierBasis,
    ) -> Result<Vec<KvCheck>, WorkspaceError> {
        validate_retained_source_binding_version(basis.source_binding.binding.binding_version)?;
        let source_keys = vec![
            packed_history_key(
                basis.source_binding.workspace_id,
                basis.source_binding.binding.binding_version,
            ),
            hot_layer_key(basis.carrier_revision.layer_id),
            GATE_KEY.to_vec(),
            registry_history_root_key(&basis.source_binding),
            registry_root_key(basis.registry_incarnation),
        ];
        let source_values = self.alias_values(&source_keys).await?;
        if source_values[0].as_deref() != Some(basis.source_binding.encode()?.as_slice())
            || source_values[3] != source_values[4]
        {
            return Err(WorkspaceError::Fenced);
        }
        let base: LayerRecord = decode_open_value(
            source_values[1].as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        if revision_from_layer(&base)? != basis.carrier_revision
            || base.parent_layer_id.is_some()
            || base.depth != 1
        {
            return Err(WorkspaceError::Fenced);
        }
        require_active_retirement_gate(&source_values[2])?;
        let root = RootRow::decode(source_values[3].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if root.incarnation != basis.registry_incarnation
            || root.state != RootState::BindingHistory
            || root.binding.as_ref() != Some(&basis.source_binding)
            || root.members == 0
            || root.pending_puts != 0
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut checks = Vec::new();
        merge(&mut checks, exact_checks(&source_keys, &source_values))?;
        Ok(checks)
    }

    pub(crate) async fn prepare_borrowed_workspace(
        &self,
        request: &CreateWorkspace,
        require_packed_carrier: bool,
    ) -> Result<BorrowedWorkspacePlan, WorkspaceError> {
        let permit = self
            .packed_reader_pin_budget
            .get()
            .map(|budget| {
                budget
                    .admit(&[(V3BudgetPool::Metadata, ALIAS_OPERATION_BYTES)])
                    .map_err(journal_budget_error)
            })
            .transpose()?;
        let keys = vec![
            packed_carrier_basis_key(request.base_revision.layer_id),
            packed_carrier_claim_key(request.base_revision.layer_id),
        ];
        let values = self.alias_values(&keys).await?;
        let mut checks = exact_checks(&keys, &values);
        let Some(basis) =
            PackedCarrierBasis::decode_pair(&request.base_revision, &values[0], &values[1])?
        else {
            if require_packed_carrier {
                return Err(WorkspaceError::Fenced);
            }
            return Ok(BorrowedWorkspacePlan {
                binding: None,
                next_inode: None,
                checks,
                writes: Vec::new(),
                _permit: permit,
            });
        };
        if permit.is_none() {
            return Err(WorkspaceError::UnsupportedCapability(
                "canonical registry budget",
            ));
        }
        let child = PackedLowerBindingRecord {
            workspace_id: request.workspace_id,
            head_layer_id: request.head_layer_id,
            head_epoch: 1,
            base_revision: request.base_revision.clone(),
            highest_inode: basis.source_binding.highest_inode,
            binding: PackedLowerBinding {
                binding_version: 1,
                base_layer_id: request.base_revision.layer_id,
                manifest: basis.source_binding.binding.manifest.clone(),
            },
        };
        let alias = BorrowedHistoryAlias {
            binding: child.clone(),
            carrier_basis_digest: Sha256::digest(values[0].as_ref().ok_or(WorkspaceError::Fenced)?)
                .into(),
            source_incarnation: basis.registry_incarnation,
        };
        merge(
            &mut checks,
            self.borrowed_history_source_checks(&child, &alias).await?,
        )?;
        let child_keys = vec![
            packed_current_key(child.workspace_id),
            packed_claim_key(child.workspace_id),
            packed_history_key(child.workspace_id, 1),
            borrowed_history_key(&child),
            registry_history_root_key(&child),
            retirement_key(&child),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            hot_allocator_key("inode"),
        ];
        let child_values = self.alias_values(&child_keys).await?;
        if child_values[..6].iter().any(Option::is_some) {
            return Err(WorkspaceError::Fenced);
        }
        let allocator: i64 = decode_required(&child_values[7])?;
        if allocator < 2 {
            return Err(WorkspaceError::Fenced);
        }
        let next_inode = allocator.max(
            child
                .highest_inode
                .checked_add(1)
                .ok_or(WorkspaceError::Fenced)?,
        );
        merge(&mut checks, exact_checks(&child_keys, &child_values))?;
        let writes = vec![
            KvWrite::Put {
                key: child_keys[0].clone(),
                value: child.encode()?,
            },
            KvWrite::Put {
                key: child_keys[1].clone(),
                value: PACKED_CLAIM.to_vec(),
            },
            KvWrite::Put {
                key: child_keys[2].clone(),
                value: child.encode()?,
            },
            KvWrite::Put {
                key: child_keys[3].clone(),
                value: alias.encode()?,
            },
            put(
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                &next_packed_root_generation(&child_values[6])?,
            )?,
            put(child_keys[7].clone(), &next_inode)?,
        ];
        Ok(BorrowedWorkspacePlan {
            binding: Some(child),
            next_inode: Some(next_inode),
            checks,
            writes,
            _permit: permit,
        })
    }

    /// Logical aliases, including Deleting/repacked children, retain the source
    /// until their own child-only retirement removes them. This supplements
    /// native ancestry without making child retirement depend on siblings.
    pub(super) async fn borrowed_history_census(
        &self,
        target: &PackedLowerBindingRecord,
        epochs: &[KvCheck],
        max_rows: u64,
        budget: &Arc<V3MountBudget>,
        cancel: &CancellationToken,
    ) -> Result<BorrowedHistoryCensus, WorkspaceError> {
        let permit = budget
            .admit(&[(V3BudgetPool::Metadata, ALIAS_OPERATION_BYTES)])
            .map_err(journal_budget_error)?;
        if max_rows == 0 {
            return Err(WorkspaceError::Busy);
        }
        let live = || {
            if budget.state().closed || cancel.is_cancelled() {
                Err(WorkspaceError::Busy)
            } else {
                Ok(())
            }
        };
        let mut after = None;
        let mut visited = 0u64;
        let mut matched = 0u64;
        let mut checks = Vec::new();
        loop {
            live()?;
            if !self.backend.compare_and_swap(epochs, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    ALIAS_PREFIX,
                    after.as_deref(),
                    KvReadLimits {
                        max_records: 8,
                        max_key_bytes: 256,
                        max_value_bytes: ALIAS_MAX_BYTES,
                        max_total_bytes: 128 << 10,
                        max_response_bytes: 128 << 10,
                        max_data_requests: 512,
                    },
                )
                .await?;
            if !self.backend.compare_and_swap(epochs, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            live()?;
            if page.is_empty() {
                return Ok(BorrowedHistoryCensus {
                    checks,
                    matched,
                    _permit: permit,
                });
            }
            for entry in page {
                visited = visited.checked_add(1).ok_or(WorkspaceError::Busy)?;
                if visited > max_rows
                    || !entry.key.starts_with(ALIAS_PREFIX)
                    || after.as_ref().is_some_and(|last| entry.key <= *last)
                {
                    return Err(WorkspaceError::Busy);
                }
                let alias = BorrowedHistoryAlias::decode(&entry.value)?;
                if entry.key != borrowed_history_key(&alias.binding) {
                    return Err(WorkspaceError::Fenced);
                }
                after = Some(entry.key.clone());
                if alias.binding.base_revision != target.base_revision {
                    continue;
                }
                merge(
                    &mut checks,
                    self.borrowed_history_source_checks(&alias.binding, &alias)
                        .await?,
                )?;
                let key = packed_history_key(alias.binding.workspace_id, 1);
                let values = self.alias_values(std::slice::from_ref(&key)).await?;
                if values[0].as_deref() != Some(alias.binding.encode()?.as_slice()) {
                    return Err(WorkspaceError::Fenced);
                }
                merge(
                    &mut checks,
                    vec![
                        KvCheck {
                            key: entry.key,
                            expected: Some(entry.value),
                        },
                        KvCheck {
                            key,
                            expected: values[0].clone(),
                        },
                    ],
                )?;
                let proof_bytes = checks
                    .iter()
                    .try_fold(0usize, |bytes, check| {
                        bytes.checked_add(check.key.len()).and_then(|bytes| {
                            bytes.checked_add(check.expected.as_ref().map_or(0, Vec::len))
                        })
                    })
                    .ok_or(WorkspaceError::Busy)?;
                if checks.len() > 512 || proof_bytes > 1 << 20 {
                    return Err(WorkspaceError::Busy);
                }
                matched = matched.checked_add(1).ok_or(WorkspaceError::Busy)?;
            }
        }
    }
}

impl<B: WorkspaceKvBackend + 'static> KvWorkspaceStore<B> {
    /// Logical retirement is permitted only at the child Deleting endpoint.
    /// The owned task and exact-successor clock CAS survive caller cancellation.
    pub(crate) async fn retire_borrowed_packed_history(
        self: &Arc<Self>,
        binding: PackedLowerBindingRecord,
        budget: Arc<V3MountBudget>,
        max_native_leases: u64,
        cancel: CancellationToken,
    ) -> Result<BorrowedHistoryRetirementReport, WorkspaceError> {
        self.configure_packed_reader_pin_budget(budget.clone())?;
        if binding.binding.binding_version != 1 || max_native_leases == 0 || max_native_leases > 512
        {
            return Err(WorkspaceError::Fenced);
        }
        let permit = budget
            .admit(&[(V3BudgetPool::Metadata, ALIAS_RETIREMENT_BYTES)])
            .map_err(journal_budget_error)?;
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = store
                .retire_borrowed_history_owned(&binding, &budget, max_native_leases, &cancel)
                .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| journal_error("borrowed history retirement driver stopped"))?
    }

    async fn retire_borrowed_history_owned(
        &self,
        binding: &PackedLowerBindingRecord,
        budget: &Arc<V3MountBudget>,
        max_native_leases: u64,
        cancel: &CancellationToken,
    ) -> Result<BorrowedHistoryRetirementReport, WorkspaceError> {
        let live = || {
            if budget.state().closed || cancel.is_cancelled() {
                Err(WorkspaceError::Busy)
            } else {
                Ok(())
            }
        };
        live()?;
        let keys = vec![
            borrowed_history_key(binding),
            packed_history_key(binding.workspace_id, 1),
            packed_current_key(binding.workspace_id),
            packed_claim_key(binding.workspace_id),
            hot_workspace_key(binding.workspace_id),
            CONTROL_KEY.to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            retirement_key(binding),
            open_v3_key(binding.workspace_id),
            open_v3_recovery_key(binding.workspace_id),
            packed_writer_key(binding.workspace_id),
            TOPOLOGY_GENERATION_KEY.to_vec(),
        ];
        let values = self.alias_values(&keys).await?;
        let mut checks = exact_checks(&keys, &values);
        let workspace: WorkspaceRecord = decode_open_value(
            values[4].as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        if workspace.workspace_id != binding.workspace_id
            || workspace.state != WorkspaceState::Deleting
            || values[10].is_some()
        {
            return Err(WorkspaceError::Busy);
        }
        let writer = PackedWriterAuthority::decode(
            values[11].as_deref().ok_or(WorkspaceError::Fenced)?,
            binding.workspace_id,
        )?;
        if writer.owner.is_some() {
            return Err(WorkspaceError::Busy);
        }
        let mut marker = b"PAR3".to_vec();
        marker.extend_from_slice(&Sha256::digest(binding.encode()?));
        let mut drop_current = false;
        if let Some(current_raw) = &values[2] {
            let current = PackedLowerBindingRecord::decode(current_raw)?;
            if current.workspace_id != binding.workspace_id
                || values[3].as_deref() != Some(PACKED_CLAIM)
            {
                return Err(WorkspaceError::Fenced);
            }
            if current.binding.binding_version == 1 {
                if &current != binding || values[0].is_none() {
                    return Err(WorkspaceError::Fenced);
                }
                drop_current = true;
            } else {
                let current_key =
                    packed_history_key(binding.workspace_id, current.binding.binding_version);
                let current_values = self
                    .alias_values(std::slice::from_ref(&current_key))
                    .await?;
                if current_values[0].as_deref() != Some(current_raw.as_slice()) {
                    return Err(WorkspaceError::Fenced);
                }
                merge(
                    &mut checks,
                    vec![KvCheck {
                        key: current_key,
                        expected: current_values[0].clone(),
                    }],
                )?;
            }
        } else if values[3].is_some() {
            return Err(WorkspaceError::Fenced);
        }
        if values[0].is_none() {
            if values[8].as_deref() != Some(marker.as_slice()) || values[1].is_some() {
                return Err(WorkspaceError::Fenced);
            }
            // An already-retired alias authorizes no further deletion. A later
            // idle open, including a live one, is retained exactly by this CAS.
            self.history_clock_cas(&checks, &[], Some(1)).await?;
            return Ok(BorrowedHistoryRetirementReport {
                retired: true,
                source_members_released: 0,
            });
        }
        if values[8].is_some() || values[1].as_deref() != Some(binding.encode()?.as_slice()) {
            return Err(WorkspaceError::Fenced);
        }
        // Only the Deleting endpoint can remove a closed open. Do not infer
        // writer retirement from expiry: the exact PWA must be idle and the
        // full native-lease census below must find every child lease Released.
        let not_before = if let Some(raw) = values[9].as_deref() {
            let open: V3OpenRecord = decode_open_value(raw, OPEN_RECORD_MAX_BYTES)?;
            validate_open_record(&open, binding.workspace_id)?;
            if open.state != V3OpenState::Ready || open.recovery_required || open.expires_at_ns <= 0
            {
                return Err(WorkspaceError::Busy);
            }
            open.expires_at_ns.max(1)
        } else {
            1
        };
        let alias =
            BorrowedHistoryAlias::decode(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        merge(
            &mut checks,
            self.borrowed_history_source_checks(binding, &alias).await?,
        )?;
        validate_current_control_raw(values[5].as_deref())?;
        let epochs = checks
            .iter()
            .filter(|check| {
                matches!(
                    check.key.as_slice(),
                    PACKED_ROOT_GENERATION_KEY
                        | LAYER_INVENTORY_GENERATION_KEY
                        | TOPOLOGY_GENERATION_KEY
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        let journal_prefix = [
            HOT_JOURNAL_PREFIX,
            format!("{}/", binding.workspace_id).as_bytes(),
        ]
        .concat();
        let mut journal_after = None;
        let mut journal_rows = 0u64;
        loop {
            live()?;
            if !self.backend.compare_and_swap(&epochs, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    &journal_prefix,
                    journal_after.as_deref(),
                    KvReadLimits {
                        max_records: 16,
                        max_key_bytes: 256,
                        max_value_bytes: OPEN_RECORD_MAX_BYTES,
                        max_total_bytes: 128 << 10,
                        max_response_bytes: 128 << 10,
                        max_data_requests: 512,
                    },
                )
                .await?;
            if !self.backend.compare_and_swap(&epochs, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            if page.is_empty() {
                break;
            }
            for entry in page {
                journal_rows = journal_rows.checked_add(1).ok_or(WorkspaceError::Busy)?;
                if journal_rows > max_native_leases
                    || !entry.key.starts_with(&journal_prefix)
                    || journal_after
                        .as_ref()
                        .is_some_and(|last| entry.key <= *last)
                {
                    return Err(WorkspaceError::Busy);
                }
                let journal: SealJournal = decode_open_value(&entry.value, OPEN_RECORD_MAX_BYTES)?;
                if journal.workspace_id != binding.workspace_id
                    || entry.key != hot_journal_key(journal.workspace_id, journal.journal_id)
                {
                    return Err(WorkspaceError::Fenced);
                }
                if !matches!(journal.phase, SealPhase::Completed | SealPhase::Aborted) {
                    return Err(WorkspaceError::Busy);
                }
                journal_after = Some(entry.key.clone());
                merge(
                    &mut checks,
                    vec![KvCheck {
                        key: entry.key,
                        expected: Some(entry.value),
                    }],
                )?;
            }
        }
        let mut after = None;
        let mut visited = 0u64;
        loop {
            live()?;
            if !self.backend.compare_and_swap(&epochs, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    HOT_LEASE_PREFIX,
                    after.as_deref(),
                    KvReadLimits {
                        max_records: 16,
                        max_key_bytes: 256,
                        max_value_bytes: OPEN_RECORD_MAX_BYTES,
                        max_total_bytes: 128 << 10,
                        max_response_bytes: 128 << 10,
                        max_data_requests: 512,
                    },
                )
                .await?;
            if !self.backend.compare_and_swap(&epochs, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            if page.is_empty() {
                break;
            }
            for entry in page {
                visited = visited.checked_add(1).ok_or(WorkspaceError::Busy)?;
                if visited > max_native_leases
                    || !entry.key.starts_with(HOT_LEASE_PREFIX)
                    || after.as_ref().is_some_and(|last| entry.key <= *last)
                {
                    return Err(WorkspaceError::Busy);
                }
                let lease: SnapshotLease = decode_open_value(&entry.value, OPEN_RECORD_MAX_BYTES)?;
                if entry.key != hot_lease_key(lease.workspace_id, lease.lease_id) {
                    return Err(WorkspaceError::Fenced);
                }
                if lease.workspace_id == binding.workspace_id && lease.state != LeaseState::Released
                {
                    return Err(WorkspaceError::Busy);
                }
                after = Some(entry.key.clone());
                merge(
                    &mut checks,
                    vec![KvCheck {
                        key: entry.key,
                        expected: Some(entry.value),
                    }],
                )?;
            }
        }
        let pins = self.packed_reader_pin_roots().await?;
        if pins
            .bindings
            .iter()
            .any(|pin| pin.workspace_id == binding.workspace_id)
        {
            return Err(WorkspaceError::Busy);
        }
        merge(&mut checks, pins.checks.clone())?;
        let (_roots, journal_checks, _journal_permit) =
            self.scan_packed_journal_layer_roots().await?;
        for check in &journal_checks {
            if check.key.starts_with(JOURNAL_PREFIX)
                && PackedJournalRecord::decode(
                    check.expected.as_deref().ok_or(WorkspaceError::Fenced)?,
                )?
                .guard
                .workspace_id
                    == binding.workspace_id
            {
                return Err(WorkspaceError::Busy);
            }
        }
        merge(&mut checks, journal_checks)?;
        live()?;
        let mut writes = vec![
            KvWrite::Delete {
                key: keys[0].clone(),
            },
            KvWrite::Delete {
                key: keys[1].clone(),
            },
            KvWrite::Put {
                key: keys[8].clone(),
                value: marker,
            },
            put(
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                &next_packed_root_generation(&values[6])?,
            )?,
        ];
        if drop_current {
            writes.push(KvWrite::Delete {
                key: keys[2].clone(),
            });
            writes.push(KvWrite::Delete {
                key: keys[3].clone(),
            });
        }
        if values[9].is_some() {
            writes.push(KvWrite::Delete {
                key: keys[9].clone(),
            });
        }
        // No source root state change, member release, object write or DELETE.
        self.history_clock_cas(&checks, &writes, Some(not_before))
            .await?;
        Ok(BorrowedHistoryRetirementReport {
            retired: true,
            source_members_released: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::validate_retained_source_binding_version;

    #[test]
    fn initial_binding_history_version_is_a_valid_retained_source() {
        assert!(validate_retained_source_binding_version(1).is_ok());
        assert!(validate_retained_source_binding_version(2).is_ok());
        assert!(validate_retained_source_binding_version(0).is_err());
    }
}
