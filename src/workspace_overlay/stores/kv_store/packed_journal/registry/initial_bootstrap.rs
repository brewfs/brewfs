//! A real first packed-v3 binding, without a fabricated PPJ or lower binding.
//! The root-only native source is repeatedly authenticated under a live lease.
//! Every PUT owns the same global registry reservation and quarantine as PPJ.

use super::*;
use crate::cadapter::client::ObjectByteStream;
use crate::cadapter::read_observer::{ReadContext, ReadObserver};
use crate::workspace_overlay::packed_v3::wire005::{
    AuthenticatedV3Snapshot, V3ProducerOptions, V3RootAttributes, V3SnapshotProducer,
};
use crate::workspace_overlay::publish::binding::VerifiedPackedLower;
use std::path::Path;

const BOOTSTRAP_BYTES: u64 = 4 << 20;
const BOOTSTRAP_MAX_OBJECTS: u64 = 64;
const BOOTSTRAP_MAX_DISK: u64 = 64 << 20;

fn bootstrap_key(workspace_id: WorkspaceId) -> Vec<u8> {
    format!("packed/v3/initial-bootstrap/{workspace_id}").into_bytes()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum BootstrapPhase {
    Building = 0,
    Candidate = 1,
    Installed = 2,
}

/// Decoded durable facts cannot construct NativeBootstrapSource authority.
#[derive(Clone, Debug, Eq, PartialEq)]
struct InitialBootstrapRecord {
    journal_id: JournalId,
    incarnation: Uuid,
    revision: u64,
    phase: BootstrapPhase,
    workspace_id: WorkspaceId,
    head_layer_id: LayerId,
    expected_head_epoch: u64,
    expected_head: Vec<u8>,
    expected_base: Vec<u8>,
    expected_root: Vec<u8>,
    source_digest: [u8; 32],
    options_digest: [u8; 32],
    object_count: u64,
    inventory_digest: [u8; 32],
    manifest: Option<V3ObjectRef>,
    binding: Option<PackedLowerBindingRecord>,
    first_admin_claim: Option<Vec<u8>>,
}

impl InitialBootstrapRecord {
    fn prefix(&self) -> String {
        format!("packed/v3/initial-stage/{}", self.incarnation.simple())
    }
    fn source_digest(&self) -> Result<[u8; 32], WorkspaceError> {
        let mut hash = Sha256::new();
        hash.update(b"BrewFS packed v3 authenticated initial native source\0");
        hash.update(self.workspace_id.as_bytes());
        hash.update(self.head_layer_id.as_bytes());
        hash.update(self.expected_head_epoch.to_le_bytes());
        for raw in [
            &self.expected_head,
            &self.expected_base,
            &self.expected_root,
        ] {
            hash.update((raw.len() as u64).to_le_bytes());
            hash.update(raw);
        }
        Ok(hash.finalize().into())
    }
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        if self.journal_id.as_uuid().is_nil()
            || self.incarnation.is_nil()
            || self.revision == 0
            || self.expected_head_epoch != 0
            || self.object_count > BOOTSTRAP_MAX_OBJECTS
            || self.source_digest != self.source_digest()?
            || self.options_digest == [0; 32]
            || ((self.phase == BootstrapPhase::Building) != self.manifest.is_none())
            || ((self.phase == BootstrapPhase::Installed) != self.binding.is_some())
            || self.first_admin_claim.as_ref().is_some_and(|raw| {
                self.phase != BootstrapPhase::Installed || raw.is_empty() || raw.len() > 4096
            })
        {
            return Err(journal_error("invalid initial bootstrap facts"));
        }
        let head: LayerRecord = decode_open_value(&self.expected_head, REFERENCE_LIMIT)?;
        let base: LayerRecord = decode_open_value(&self.expected_base, REFERENCE_LIMIT)?;
        let root: InodeDelta = decode_open_value(&self.expected_root, REFERENCE_LIMIT)?;
        validate_initial_native(self.workspace_id, self.head_layer_id, &head, &base, &root)?;
        if self.manifest.as_ref().is_some_and(|reference| {
            reference.kind != V3ObjectKind::Manifest
                || !reference.key.starts_with(&format!("{}/", self.prefix()))
        }) {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(binding) = &self.binding
            && (binding.workspace_id != self.workspace_id
                || binding.head_layer_id != self.head_layer_id
                || binding.head_epoch != 1
                || binding.binding.binding_version != 1
                || binding.highest_inode != 1
                || binding.base_revision != revision_from_layer(&base)?
                || Some(&binding.binding.manifest) != self.manifest.as_ref())
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut bytes = row_header(b"PBI3");
        bytes.extend_from_slice(self.journal_id.as_bytes());
        bytes.extend_from_slice(self.incarnation.as_bytes());
        bytes.extend_from_slice(&self.revision.to_le_bytes());
        bytes.push(self.phase as u8);
        bytes.extend_from_slice(self.workspace_id.as_bytes());
        bytes.extend_from_slice(self.head_layer_id.as_bytes());
        bytes.extend_from_slice(&self.expected_head_epoch.to_le_bytes());
        for raw in [
            &self.expected_head,
            &self.expected_base,
            &self.expected_root,
        ] {
            append_bytes(&mut bytes, raw, REFERENCE_LIMIT)?;
        }
        bytes.extend_from_slice(&self.source_digest);
        bytes.extend_from_slice(&self.options_digest);
        bytes.extend_from_slice(&self.object_count.to_le_bytes());
        bytes.extend_from_slice(&self.inventory_digest);
        append_bytes(
            &mut bytes,
            &self
                .manifest
                .as_ref()
                .map(V3ObjectRef::encode_value)
                .transpose()
                .map_err(journal_error)?
                .unwrap_or_default(),
            REFERENCE_LIMIT,
        )?;
        append_bytes(
            &mut bytes,
            &self
                .binding
                .as_ref()
                .map(PackedLowerBindingRecord::encode)
                .transpose()?
                .unwrap_or_default(),
            REFERENCE_LIMIT,
        )?;
        append_bytes(
            &mut bytes,
            self.first_admin_claim.as_deref().unwrap_or_default(),
            4096,
        )?;
        finish_record(bytes, RECORD_LIMIT)
    }
    fn decode(raw: &[u8]) -> Result<Self, WorkspaceError> {
        let mut cursor = JournalCursor::checked(raw, b"PBI3", RECORD_LIMIT)?;
        let journal_id = JournalId::from_uuid(Uuid::from_bytes(cursor.take()?));
        let incarnation = Uuid::from_bytes(cursor.take()?);
        let revision = cursor.u64()?;
        let phase = match cursor.take::<1>()?[0] {
            0 => BootstrapPhase::Building,
            1 => BootstrapPhase::Candidate,
            2 => BootstrapPhase::Installed,
            _ => return Err(journal_error("unknown initial bootstrap phase")),
        };
        let workspace_id = WorkspaceId::from_uuid(Uuid::from_bytes(cursor.take()?));
        let head_layer_id = LayerId::from_uuid(Uuid::from_bytes(cursor.take()?));
        let expected_head_epoch = cursor.u64()?;
        let expected_head = cursor.bytes(REFERENCE_LIMIT)?;
        let expected_base = cursor.bytes(REFERENCE_LIMIT)?;
        let expected_root = cursor.bytes(REFERENCE_LIMIT)?;
        let source_digest = cursor.take()?;
        let options_digest = cursor.take()?;
        let object_count = cursor.u64()?;
        let inventory_digest = cursor.take()?;
        let manifest_raw = cursor.bytes(REFERENCE_LIMIT)?;
        let binding_raw = cursor.bytes(REFERENCE_LIMIT)?;
        let claim_raw = cursor.bytes(4096)?;
        cursor.end()?;
        let record = Self {
            journal_id,
            incarnation,
            revision,
            phase,
            workspace_id,
            head_layer_id,
            expected_head_epoch,
            expected_head,
            expected_base,
            expected_root,
            source_digest,
            options_digest,
            object_count,
            inventory_digest,
            manifest: if manifest_raw.is_empty() {
                None
            } else {
                Some(V3ObjectRef::decode_value(&manifest_raw).map_err(journal_error)?)
            },
            binding: if binding_raw.is_empty() {
                None
            } else {
                Some(PackedLowerBindingRecord::decode(&binding_raw)?)
            },
            first_admin_claim: (!claim_raw.is_empty()).then_some(claim_raw),
        };
        record.encode()?;
        Ok(record)
    }
    fn next(&self) -> Result<Self, WorkspaceError> {
        let mut next = self.clone();
        next.revision = increment(next.revision)?;
        Ok(next)
    }
}

fn validate_initial_native(
    workspace: WorkspaceId,
    head_id: LayerId,
    head: &LayerRecord,
    base: &LayerRecord,
    root: &InodeDelta,
) -> Result<(), WorkspaceError> {
    crate::workspace_overlay::resolver::validate_layer_chain(
        head_id,
        &[head.clone(), base.clone()],
    )?;
    if head.layer_id != head_id
        || head.state != LayerState::Writable
        || head.owner_workspace_id != Some(workspace)
        || head.next_sequence != 1
        || head.owned_slice_count != 0
        || head.owned_bytes != 0
        || base.parent_layer_id.is_some()
        || base.state != LayerState::Sealed
        || base.sealed_version != Some(1)
        || base.next_sequence != 2
        || base.owned_slice_count != 0
        || base.owned_bytes != 0
        || root.layer_id != base.layer_id
        || root.ino != 1
        || root.state != InodeState::Present
        || root.kind != 1
        || root.size != 0
        || root.sequence != 1
        || root.parent_hint != Some(1)
        || root.symlink_target.is_some()
    {
        return Err(WorkspaceError::UnsupportedCapability(
            "first packed binding requires root-only native source",
        ));
    }
    let digest = delta_digest(&CanonicalLayerDelta {
        inodes: vec![root.clone()],
        ..Default::default()
    })?;
    if base.delta_digest != Some(digest) || base.root_hash != Some(root_hash([0; 32], digest)) {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
}

fn root_attributes(root: &InodeDelta) -> V3RootAttributes {
    V3RootAttributes {
        inode: 1,
        size: root.size,
        blocks: 0,
        mode: 0o040000 | root.mode,
        uid: root.uid,
        gid: root.gid,
        nlink: root.nlink,
        atime_ns: root.atime_ns,
        mtime_ns: root.mtime_ns,
        ctime_ns: root.ctime_ns,
    }
}

fn options_digest(options: &V3ProducerOptions) -> Result<[u8; 32], WorkspaceError> {
    if options.root_inode != 1 || options.snapshot_id == [0; 32] || options.root_dir_key == [0; 32]
    {
        return Err(WorkspaceError::Fenced);
    }
    options.size_classes.validate().map_err(journal_error)?;
    options
        .build_policy
        .select(1, options.profile, options.size_classes)
        .map_err(journal_budget_error)?;
    // This internal retry identity includes every producer option. A binary
    // whose option representation changes rejects a reopen rather than silently
    // adopting different bytes under an existing create-only object key.
    let mut hash = Sha256::new();
    hash.update(b"BrewFS packed v3 initial producer options\0");
    hash.update(format!("{options:?}").as_bytes());
    Ok(hash.finalize().into())
}

fn merge(checks: &mut Vec<KvCheck>, added: Vec<KvCheck>) -> Result<(), WorkspaceError> {
    super::super::native_publication::append_exact_checks(checks, added)?;
    let mut bytes = 0usize;
    for check in checks.iter() {
        let length = check.expected.as_ref().map_or(0, Vec::len);
        if check.key.len() > 256 || length > RECORD_LIMIT {
            return Err(WorkspaceError::Fenced);
        }
        bytes = bytes
            .checked_add(check.key.len())
            .and_then(|n| n.checked_add(length))
            .ok_or(WorkspaceError::Busy)?;
    }
    if checks.len() > 256 || bytes > 2 << 20 {
        return Err(WorkspaceError::Busy);
    }
    Ok(())
}

fn exact(keys: &[Vec<u8>], values: &[Option<Vec<u8>>]) -> Vec<KvCheck> {
    keys.iter()
        .cloned()
        .zip(values.iter().cloned())
        .map(|(key, expected)| KvCheck { key, expected })
        .collect()
}

/// Only a fresh bounded real source read plus a timed backend CAS issues this.
struct NativeBootstrapSource {
    checks: Vec<KvCheck>,
    workspace: WorkspaceRecord,
    head: LayerRecord,
    base: LayerRecord,
    root: InodeDelta,
    lease: SnapshotLease,
    now: i64,
    _permit: V3OwnedPermit,
}

impl<K: WorkspaceKvBackend> KvWorkspaceStore<K> {
    async fn bootstrap_values(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
        let mut all = Vec::with_capacity(keys.len());
        let mut time = 0;
        for batch in keys.chunks(2) {
            let (values, now) = self
                .backend
                .get_many_consistent_with_time_bounded(
                    batch,
                    KvReadLimits {
                        max_records: 2,
                        max_data_requests: 2,
                        max_total_bytes: 96 << 10,
                        max_response_bytes: 128 << 10,
                        ..journal_point_limits()
                    },
                )
                .await?;
            if values.len() != batch.len() || now <= 0 {
                return Err(WorkspaceError::Fenced);
            }
            all.extend(values);
            time = now;
        }
        Ok((all, time))
    }

    async fn authenticate_initial_native_source(
        &self,
        guard: &HeadGuard,
        expected: Option<&InitialBootstrapRecord>,
        budget: &Arc<V3MountBudget>,
    ) -> Result<NativeBootstrapSource, WorkspaceError> {
        let permit = budget
            .admit(&[(V3BudgetPool::Metadata, BOOTSTRAP_BYTES)])
            .map_err(journal_budget_error)?;
        if budget.state().closed || guard.expected_head_epoch != 0 {
            return Err(WorkspaceError::Fenced);
        }
        let (first, _) = self
            .bootstrap_values(&[hot_layer_key(guard.expected_head_layer_id)])
            .await?;
        let first_head: LayerRecord = decode_required(&first[0])?;
        let base_id = first_head.parent_layer_id.ok_or(WorkspaceError::Fenced)?;
        let keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(guard.workspace_id),
            hot_layer_key(guard.expected_head_layer_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            hot_layer_key(base_id),
            hot_allocator_key("inode"),
            packed_current_key(guard.workspace_id),
            packed_claim_key(guard.workspace_id),
            packed_history_key(guard.workspace_id, 1),
            inode_identity_key(base_id, 1),
            open_v3_key(guard.workspace_id),
            open_v3_recovery_key(guard.workspace_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
        ];
        let (values, now) = self.bootstrap_values(&keys).await?;
        validate_current_control_raw(values[0].as_deref())?;
        let workspace: WorkspaceRecord = decode_required(&values[1])?;
        let head: LayerRecord = decode_required(&values[2])?;
        let lease: SnapshotLease = decode_required(&values[3])?;
        let base: LayerRecord = decode_required(&values[4])?;
        let allocator: i64 = decode_required(&values[5])?;
        let root: InodeDelta = decode_required(&values[9])?;
        checked_hot_guard(&workspace, &head, &lease, guard, now)?;
        validate_initial_native(
            guard.workspace_id,
            guard.expected_head_layer_id,
            &head,
            &base,
            &root,
        )?;
        if allocator != 2
            || values[6..9].iter().any(Option::is_some)
            || values[10..12].iter().any(Option::is_some)
            || head != first_head
            || lease.base_revision != revision_from_layer(&base)?
            || workspace.state != WorkspaceState::Active
            || workspace.active_lease != Some(guard.lease_id)
        {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(record) = expected
            && (record.workspace_id != guard.workspace_id
                || record.head_layer_id != guard.expected_head_layer_id
                || record.expected_head_epoch != guard.expected_head_epoch
                || values[2].as_deref() != Some(record.expected_head.as_slice())
                || values[4].as_deref() != Some(record.expected_base.as_slice())
                || values[9].as_deref() != Some(record.expected_root.as_slice()))
        {
            return Err(WorkspaceError::Fenced);
        }
        let checks = exact(&keys, &values);
        // Every native write changes the head sequence in its own CAS. Sealed
        // base rows are immutable. Bracketing every native family authenticates
        // absence; a nonempty source is never masked by an empty manifest.
        for layer in [head.layer_id, base.layer_id] {
            for (kind, prefix) in [
                (0, inode_layer_prefix(layer)),
                (1, dentry_layer_prefix(layer)),
                (2, xattr_layer_prefix(layer)),
                (3, acl_layer_prefix(layer)),
                (4, extent_layer_prefix(layer)),
                (5, native_reverse::layer_prefix(layer)),
                (6, extent_owner_layer_prefix(layer)),
            ] {
                if !self
                    .backend
                    .compare_and_swap_before(&checks, &[], lease.expires_at_ns)
                    .await?
                {
                    return Err(WorkspaceError::Busy);
                }
                let page = self
                    .backend
                    .scan_prefix_page_with_byte_limits(
                        &prefix,
                        None,
                        KvReadLimits {
                            max_records: 2,
                            max_key_bytes: 256,
                            max_value_bytes: RECORD_LIMIT,
                            max_total_bytes: 96 << 10,
                            max_response_bytes: 128 << 10,
                            max_data_requests: 2,
                        },
                    )
                    .await?;
                if !self
                    .backend
                    .compare_and_swap_before(&checks, &[], lease.expires_at_ns)
                    .await?
                {
                    return Err(WorkspaceError::Busy);
                }
                if layer == base.layer_id && kind == 0 {
                    if page.len() != 1
                        || page[0].key != keys[9]
                        || Some(page[0].value.as_slice()) != values[9].as_deref()
                    {
                        return Err(WorkspaceError::UnsupportedCapability(
                            "nonempty initial native source",
                        ));
                    }
                    // TiKV can return a short nonempty page at its RPC bound.
                    // Only the following actual empty page proves the base
                    // inode census contains exactly this one native root.
                    let tail = self
                        .backend
                        .scan_prefix_page_with_byte_limits(
                            &prefix,
                            Some(&page[0].key),
                            KvReadLimits {
                                max_records: 2,
                                max_key_bytes: 256,
                                max_value_bytes: RECORD_LIMIT,
                                max_total_bytes: 96 << 10,
                                max_response_bytes: 128 << 10,
                                max_data_requests: 2,
                            },
                        )
                        .await?;
                    if !self
                        .backend
                        .compare_and_swap_before(&checks, &[], lease.expires_at_ns)
                        .await?
                    {
                        return Err(WorkspaceError::Busy);
                    }
                    if !tail.is_empty() {
                        return Err(WorkspaceError::UnsupportedCapability(
                            "nonempty initial native source",
                        ));
                    }
                } else if !page.is_empty() {
                    return Err(WorkspaceError::UnsupportedCapability(
                        "nonempty initial native source",
                    ));
                }
            }
        }
        Ok(NativeBootstrapSource {
            checks,
            workspace,
            head,
            base,
            root,
            lease,
            now,
            _permit: permit,
        })
    }

    async fn bootstrap_write(
        &self,
        guard: &HeadGuard,
        expected: &InitialBootstrapRecord,
        next: &InitialBootstrapRecord,
        changes: &[RowChange],
        budget: &Arc<V3MountBudget>,
    ) -> Result<(), WorkspaceError> {
        let source = self
            .authenticate_initial_native_source(guard, Some(expected), budget)
            .await?;
        let mut checks = source.checks.clone();
        merge(
            &mut checks,
            vec![KvCheck {
                key: bootstrap_key(expected.workspace_id),
                expected: Some(expected.encode()?),
            }],
        )?;
        let mut writes = vec![KvWrite::Put {
            key: bootstrap_key(next.workspace_id),
            value: next.encode()?,
        }];
        for (key, old, new) in changes {
            merge(
                &mut checks,
                vec![KvCheck {
                    key: key.clone(),
                    expected: old.clone(),
                }],
            )?;
            match new {
                Some(value) => writes.push(KvWrite::Put {
                    key: key.clone(),
                    value: value.clone(),
                }),
                None => writes.push(KvWrite::Delete { key: key.clone() }),
            }
        }
        if !self
            .backend
            .compare_and_swap_before(&checks, &writes, source.lease.expires_at_ns)
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }

    async fn begin_initial_bootstrap(
        &self,
        guard: &HeadGuard,
        options: &V3ProducerOptions,
        budget: &Arc<V3MountBudget>,
    ) -> Result<InitialBootstrapRecord, WorkspaceError> {
        let _owner = budget
            .admit(&[(V3BudgetPool::Metadata, BOOTSTRAP_BYTES)])
            .map_err(journal_budget_error)?;
        let key = bootstrap_key(guard.workspace_id);
        let (values, _) = self.bootstrap_values(std::slice::from_ref(&key)).await?;
        if let Some(raw) = &values[0] {
            let record = InitialBootstrapRecord::decode(raw)?;
            if record.workspace_id != guard.workspace_id
                || record.head_layer_id != guard.expected_head_layer_id
                || record.options_digest != options_digest(options)?
            {
                return Err(WorkspaceError::Fenced);
            }
            if record.phase != BootstrapPhase::Installed {
                let source = self
                    .authenticate_initial_native_source(guard, Some(&record), budget)
                    .await?;
                let mut checks = source.checks.clone();
                merge(&mut checks, exact(std::slice::from_ref(&key), &values))?;
                if !self
                    .backend
                    .compare_and_swap_before(&checks, &[], source.lease.expires_at_ns)
                    .await?
                {
                    return Err(WorkspaceError::Busy);
                }
            }
            return Ok(record);
        }
        let source = self
            .authenticate_initial_native_source(guard, None, budget)
            .await?;
        let mut record = InitialBootstrapRecord {
            journal_id: JournalId::new(),
            incarnation: Uuid::new_v4(),
            revision: 1,
            phase: BootstrapPhase::Building,
            workspace_id: guard.workspace_id,
            head_layer_id: guard.expected_head_layer_id,
            expected_head_epoch: guard.expected_head_epoch,
            expected_head: encode(&source.head)?,
            expected_base: encode(&source.base)?,
            expected_root: encode(&source.root)?,
            source_digest: [0; 32],
            options_digest: options_digest(options)?,
            object_count: 0,
            inventory_digest: inventory_start(),
            manifest: None,
            binding: None,
            first_admin_claim: None,
        };
        record.source_digest = record.source_digest()?;
        let root = RootRow {
            journal_id: record.journal_id,
            incarnation: record.incarnation,
            revision: 1,
            state: RootState::Staging,
            members: 0,
            pending_puts: 0,
            binding: None,
        };
        let mut checks = source.checks.clone();
        merge(
            &mut checks,
            vec![
                KvCheck {
                    key: key.clone(),
                    expected: None,
                },
                KvCheck {
                    key: registry_root_key(record.incarnation),
                    expected: None,
                },
            ],
        )?;
        let writes = [
            KvWrite::Put {
                key,
                value: record.encode()?,
            },
            KvWrite::Put {
                key: registry_root_key(record.incarnation),
                value: root.encode()?,
            },
        ];
        if !self
            .backend
            .compare_and_swap_before(&checks, &writes, source.lease.expires_at_ns)
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(record)
    }

    async fn bootstrap_registered_member(
        &self,
        record: &InitialBootstrapRecord,
        reference: &V3ObjectRef,
    ) -> Result<(Vec<KvCheck>, Option<MemberRow>), WorkspaceError> {
        let keys = [
            bootstrap_key(record.workspace_id),
            registry_object_key(reference),
            registry_member_key(reference, record.incarnation),
            registry_root_key(record.incarnation),
            identity_key(record.journal_id, &reference.key),
        ];
        let (values, _) = self.bootstrap_values(&keys).await?;
        if values[0].as_deref() != Some(record.encode()?.as_slice()) {
            return Err(WorkspaceError::Busy);
        }
        let root = RootRow::decode(values[3].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if root.journal_id != record.journal_id
            || root.incarnation != record.incarnation
            || root.members != record.object_count
            || root.binding.as_ref() != record.binding.as_ref()
            || (record.phase != BootstrapPhase::Building && root.pending_puts != 0)
            || root.state
                != if record.phase == BootstrapPhase::Installed {
                    RootState::BindingHistory
                } else {
                    RootState::Staging
                }
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut checks = exact(&keys, &values);
        let Some(raw_member) = &values[2] else {
            if values[4].is_some() {
                return Err(WorkspaceError::Fenced);
            }
            return Ok((checks, None));
        };
        let object = ObjectRow::decode(values[1].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        let member = MemberRow::decode(raw_member)?;
        if object.reference != *reference
            || object.state != ObjectState::Live
            || member.reference != *reference
            || member.journal_id != record.journal_id
            || member.incarnation != record.incarnation
            || member.ordinal >= record.object_count
            || !member.retained
            || (member.adopted != (record.phase == BootstrapPhase::Installed))
            || (member.adopted && (!member.put_id.is_nil() || member.dispatched))
            || (member.pending_put && (object.pending_puts == 0 || root.pending_puts == 0))
            || values[4].as_deref() != Some(member.ordinal.to_le_bytes().as_slice())
        {
            return Err(WorkspaceError::Fenced);
        }
        let extra_keys = [
            registry_reverse_key(record.incarnation, member.ordinal),
            object_key(record.journal_id, member.ordinal),
        ];
        let (extra, _) = self.bootstrap_values(&extra_keys).await?;
        let occurrence =
            PackedJournalObject::decode(extra[1].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if extra[0].as_deref() != Some(reference.encode_value().map_err(journal_error)?.as_slice())
            || occurrence.reference != *reference
            || occurrence.ordinal != member.ordinal
            || occurrence.uploaded == member.pending_put
            || occurrence.readback_recorded
        {
            return Err(WorkspaceError::Fenced);
        }
        merge(&mut checks, exact(&extra_keys, &extra))?;
        Ok((checks, Some(member)))
    }

    async fn reserve_initial_upload(
        &self,
        guard: &HeadGuard,
        expected: &InitialBootstrapRecord,
        reference: V3ObjectRef,
        budget: &Arc<V3MountBudget>,
    ) -> Result<(InitialBootstrapRecord, PackedUploadGuard), WorkspaceError> {
        let token_owner = budget
            .admit(&[(V3BudgetPool::Metadata, 32 << 10)])
            .map_err(journal_budget_error)?;
        if expected.phase != BootstrapPhase::Building
            || expected.object_count >= BOOTSTRAP_MAX_OBJECTS
        {
            return Err(WorkspaceError::Busy);
        }
        let keys = [
            registry_object_key(&reference),
            registry_member_key(&reference, expected.incarnation),
            registry_root_key(expected.incarnation),
            registry_reverse_key(expected.incarnation, expected.object_count),
        ];
        let (values, _) = self.bootstrap_values(&keys).await?;
        if values[1].is_some() || values[3].is_some() {
            return Err(WorkspaceError::Fenced);
        }
        let mut root = RootRow::decode(values[2].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if root.state != RootState::Staging
            || root.journal_id != expected.journal_id
            || root.incarnation != expected.incarnation
            || root.members != expected.object_count
            || root.binding.is_some()
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut object = values[0]
            .as_deref()
            .map(ObjectRow::decode)
            .transpose()?
            .unwrap_or(ObjectRow {
                reference: reference.clone(),
                revision: 0,
                state: ObjectState::Live,
                memberships: 0,
                pending_puts: 0,
                delete_id: Uuid::nil(),
                delete_dispatched: false,
            });
        if object.reference != reference || object.state != ObjectState::Live {
            return Err(WorkspaceError::Fenced);
        }
        object.revision = increment(object.revision)?;
        object.memberships = increment(object.memberships)?;
        object.pending_puts = increment(object.pending_puts)?;
        root.revision = increment(root.revision)?;
        root.members = increment(root.members)?;
        root.pending_puts = increment(root.pending_puts)?;
        let put_id = Uuid::new_v4();
        let ordinal = expected.object_count;
        let member = MemberRow {
            reference: reference.clone(),
            journal_id: expected.journal_id,
            incarnation: expected.incarnation,
            ordinal,
            put_id,
            adopted: false,
            pending_put: true,
            dispatched: false,
            retained: true,
        };
        let occurrence = PackedJournalObject {
            ordinal,
            reference: reference.clone(),
            uploaded: false,
            readback_recorded: false,
        };
        let mut next = expected.next()?;
        next.object_count = increment(next.object_count)?;
        next.inventory_digest = inventory_append(expected.inventory_digest, ordinal, &reference)?;
        let changes = vec![
            (keys[0].clone(), values[0].clone(), Some(object.encode()?)),
            (keys[1].clone(), None, Some(member.encode()?)),
            (keys[2].clone(), values[2].clone(), Some(root.encode()?)),
            (
                keys[3].clone(),
                None,
                Some(reference.encode_value().map_err(journal_error)?),
            ),
            (
                object_key(expected.journal_id, ordinal),
                None,
                Some(occurrence.encode()?),
            ),
            (
                identity_key(expected.journal_id, &reference.key),
                None,
                Some(ordinal.to_le_bytes().to_vec()),
            ),
        ];
        self.bootstrap_write(guard, expected, &next, &changes, budget)
            .await?;
        Ok((
            next,
            PackedUploadGuard {
                reference,
                incarnation: expected.incarnation,
                journal_id: expected.journal_id,
                ordinal,
                put_id,
                _permit: token_owner,
            },
        ))
    }

    async fn change_initial_upload(
        &self,
        source_guard: &HeadGuard,
        expected: &InitialBootstrapRecord,
        upload: &PackedUploadGuard,
        completed: bool,
        budget: &Arc<V3MountBudget>,
    ) -> Result<InitialBootstrapRecord, WorkspaceError> {
        let _owner = budget
            .admit(&[(V3BudgetPool::Metadata, BOOTSTRAP_BYTES)])
            .map_err(journal_budget_error)?;
        if expected.phase != BootstrapPhase::Building
            || upload.journal_id != expected.journal_id
            || upload.incarnation != expected.incarnation
        {
            return Err(WorkspaceError::Fenced);
        }
        let (proof, member) = self
            .bootstrap_registered_member(expected, &upload.reference)
            .await?;
        let mut member = member.ok_or(WorkspaceError::Fenced)?;
        if member.put_id != upload.put_id
            || member.ordinal != upload.ordinal
            || !member.pending_put
            || member.dispatched != completed
        {
            return Err(WorkspaceError::Fenced);
        }
        let values = |key: &[u8]| {
            proof
                .iter()
                .find(|check| check.key == key)
                .and_then(|check| check.expected.clone())
                .ok_or(WorkspaceError::Fenced)
        };
        let object_key = registry_object_key(&upload.reference);
        let root_key = registry_root_key(upload.incarnation);
        let member_key = registry_member_key(&upload.reference, upload.incarnation);
        let occurrence_key = super::super::object_key(expected.journal_id, upload.ordinal);
        let mut object = ObjectRow::decode(&values(&object_key)?)?;
        let mut root = RootRow::decode(&values(&root_key)?)?;
        let mut occurrence = PackedJournalObject::decode(&values(&occurrence_key)?)?;
        let mut changes: Vec<RowChange> = proof
            .iter()
            .map(|check| {
                (
                    check.key.clone(),
                    check.expected.clone(),
                    check.expected.clone(),
                )
            })
            .collect();
        // The bootstrap journal is written exactly once by bootstrap_write.
        changes.retain(|change| change.0 != bootstrap_key(expected.workspace_id));
        member.dispatched = true;
        if completed {
            member.pending_put = false;
            object.revision = increment(object.revision)?;
            object.pending_puts = decrement(object.pending_puts)?;
            root.revision = increment(root.revision)?;
            root.pending_puts = decrement(root.pending_puts)?;
            occurrence.uploaded = true;
        }
        for (key, value) in [
            (object_key, object.encode()?),
            (root_key, root.encode()?),
            (member_key, member.encode()?),
            (occurrence_key, occurrence.encode()?),
        ] {
            changes
                .iter_mut()
                .find(|change| change.0 == key)
                .ok_or(WorkspaceError::Fenced)?
                .2 = Some(value);
        }
        let next = expected.next()?;
        self.bootstrap_write(source_guard, expected, &next, &changes, budget)
            .await?;
        Ok(next)
    }
}

struct InitialStagedBackend<O: ObjectBackend + Clone, K: WorkspaceKvBackend> {
    inner: O,
    store: Arc<KvWorkspaceStore<K>>,
    state: Arc<tokio::sync::Mutex<InitialBootstrapRecord>>,
    guard: HeadGuard,
    budget: Arc<V3MountBudget>,
}
impl<O: ObjectBackend + Clone, K: WorkspaceKvBackend> Clone for InitialStagedBackend<O, K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            store: self.store.clone(),
            state: self.state.clone(),
            guard: self.guard.clone(),
            budget: self.budget.clone(),
        }
    }
}
#[async_trait]
impl<O: ObjectBackend + Clone + 'static, K: WorkspaceKvBackend + 'static> ObjectBackend
    for InitialStagedBackend<O, K>
{
    async fn put_object(&self, _: &str, _: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("initial packed objects require guarded create-only PUT")
    }
    async fn put_object_create_only(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        let _owner = self
            .budget
            .admit(&[(V3BudgetPool::Metadata, BOOTSTRAP_BYTES)])
            .map_err(journal_budget_error)?;
        let reference = super::upload_backend::typed_reference(key, data)?;
        let mut state = self.state.lock().await;
        if !key.starts_with(&format!("{}/", state.prefix())) {
            anyhow::bail!("initial PUT escaped staging incarnation");
        }
        let (proof, member) = self
            .store
            .bootstrap_registered_member(&state, &reference)
            .await?;
        if let Some(member) = member {
            if member.pending_put {
                anyhow::bail!("initial PUT retains an unresolved remote outcome");
            }
            let source = self
                .store
                .authenticate_initial_native_source(&self.guard, Some(&state), &self.budget)
                .await?;
            let mut checks = source.checks.clone();
            merge(&mut checks, proof)?;
            if !self
                .store
                .backend
                .compare_and_swap_before(&checks, &[], source.lease.expires_at_ns)
                .await?
            {
                anyhow::bail!("initial completed upload retry lost source authority");
            }
            return Ok(());
        }
        let (reserved, upload) = self
            .store
            .reserve_initial_upload(&self.guard, &state, reference, &self.budget)
            .await?;
        *state = reserved;
        *state = self
            .store
            .change_initial_upload(&self.guard, &state, &upload, false, &self.budget)
            .await?;
        // Lost dispatch acknowledgements do not reach this physical call.
        // Actual PUT error/drop leaves object/root/member pending holds intact.
        self.inner.put_object_create_only(key, data).await?;
        *state = self
            .store
            .change_initial_upload(&self.guard, &state, &upload, true, &self.budget)
            .await?;
        Ok(())
    }
    async fn get_object(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get_object(key).await
    }
    async fn get_object_stream(&self, key: &str) -> anyhow::Result<Option<ObjectByteStream>> {
        self.inner.get_object_stream(key).await
    }
    async fn get_object_stream_observed(
        &self,
        key: &str,
        expected: Option<u64>,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<Option<ObjectByteStream>> {
        self.inner
            .get_object_stream_observed(key, expected, context, observer)
            .await
    }
    async fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        buf: &mut [u8],
    ) -> anyhow::Result<usize> {
        self.inner.get_object_range(key, offset, buf).await
    }
    async fn get_object_range_stream(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> anyhow::Result<ObjectByteStream> {
        self.inner
            .get_object_range_stream(key, offset, length)
            .await
    }
    async fn get_object_range_stream_observed(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<ObjectByteStream> {
        self.inner
            .get_object_range_stream_observed(key, offset, length, context, observer)
            .await
    }
    async fn get_object_size(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size(key).await
    }
    async fn get_object_size_bounded(&self, key: &str) -> anyhow::Result<Option<u64>> {
        self.inner.get_object_size_bounded(key).await
    }
    async fn get_object_size_bounded_observed(
        &self,
        key: &str,
        context: ReadContext,
        observer: Arc<ReadObserver>,
    ) -> anyhow::Result<Option<u64>> {
        self.inner
            .get_object_size_bounded_observed(key, context, observer)
            .await
    }
    async fn get_etag(&self, key: &str) -> anyhow::Result<String> {
        self.inner.get_etag(key).await
    }
    async fn delete_object(&self, _: &str) -> anyhow::Result<()> {
        anyhow::bail!("initial staged DELETE requires an exact registry guard")
    }
}

struct InitialGraphMembers<'a, K: WorkspaceKvBackend> {
    store: &'a KvWorkspaceStore<K>,
    record: &'a InitialBootstrapRecord,
    budget: &'a Arc<V3MountBudget>,
}
#[async_trait]
impl<K: WorkspaceKvBackend> V3StagedObjectVerifier for InitialGraphMembers<'_, K> {
    async fn verify_reference(
        &self,
        reference: &V3ObjectRef,
    ) -> crate::workspace_overlay::packed_v3::PackedResult<()> {
        let result = async {
            let _owner = self
                .budget
                .admit(&[(V3BudgetPool::Metadata, BOOTSTRAP_BYTES)])
                .map_err(journal_budget_error)?;
            let (checks, member) = self
                .store
                .bootstrap_registered_member(self.record, reference)
                .await?;
            if member
                .is_none_or(|member| member.pending_put || (!member.adopted && !member.dispatched))
            {
                return Err(WorkspaceError::Fenced);
            }
            if !self.store.backend.compare_and_swap(&checks, &[]).await? {
                return Err(WorkspaceError::Busy);
            }
            Ok(())
        }
        .await;
        result.map_err(|error: WorkspaceError| {
            crate::workspace_overlay::packed_v3::PackedWireError::Backend(error.to_string())
        })
    }
}

impl<K: WorkspaceKvBackend + 'static> KvWorkspaceStore<K> {
    /// Fresh root-only native volume -> actual registered graph -> first PWB3.
    /// The owned driver keeps pending remote operations alive on caller drop.
    pub async fn bootstrap_initial_packed_lower<O: ObjectBackend + Clone + 'static>(
        self: &Arc<Self>,
        guard: HeadGuard,
        client: ObjectClient<O>,
        temporary: std::path::PathBuf,
        options: V3ProducerOptions,
        budget: Arc<V3MountBudget>,
        cancel: CancellationToken,
    ) -> Result<PackedLowerBindingRecord, WorkspaceError> {
        self.require_admin_access()?;
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            WorkspaceError::Backend("initial packed bootstrap requires a Tokio runtime".into())
        })?;
        self.configure_packed_reader_pin_budget(budget.clone())?;
        let permit = budget
            .admit(&[(V3BudgetPool::Metadata, BOOTSTRAP_BYTES)])
            .map_err(journal_budget_error)?;
        let store = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        runtime.spawn(async move {
            let _permit = permit;
            let result = store
                .bootstrap_initial_packed_owned(guard, client, temporary, options, budget, cancel)
                .await;
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| journal_error("initial packed bootstrap driver stopped"))?
    }

    async fn audit_initial_bootstrap<O: ObjectBackend + Clone>(
        &self,
        record: &InitialBootstrapRecord,
        client: &ObjectClient<O>,
        temporary: &Path,
        budget: &Arc<V3MountBudget>,
        cancel: &CancellationToken,
    ) -> Result<V3IndexContextAudit, WorkspaceError> {
        let mut inventory = inventory_start();
        for ordinal in 0..record.object_count {
            if cancel.is_cancelled() || budget.state().closed {
                return Err(WorkspaceError::Busy);
            }
            let (values, _) = self
                .bootstrap_values(&[object_key(record.journal_id, ordinal)])
                .await?;
            let occurrence =
                PackedJournalObject::decode(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
            if occurrence.ordinal != ordinal || !occurrence.uploaded || occurrence.readback_recorded
            {
                return Err(WorkspaceError::Fenced);
            }
            inventory = inventory_append(inventory, ordinal, &occurrence.reference)?;
        }
        if inventory != record.inventory_digest || record.object_count == 0 {
            return Err(WorkspaceError::Fenced);
        }
        let manifest = record.manifest.as_ref().ok_or(WorkspaceError::Fenced)?;
        let verifier = InitialGraphMembers {
            store: self,
            record,
            budget,
        };
        let graph = audit_v3_staged_index_contexts(
            client,
            manifest,
            temporary,
            budget.clone(),
            V3IndexAuditLimits {
                max_objects: BOOTSTRAP_MAX_OBJECTS,
                max_disk_bytes: BOOTSTRAP_MAX_DISK,
                max_authenticated_bytes: 64 << 20,
                max_requested_bytes: 64 << 20,
                max_decoded_bytes: 64 << 20,
                ..Default::default()
            },
            cancel.clone(),
            &verifier,
        )
        .await
        .map_err(journal_budget_error)?;
        let (_, root_inode, highest_inode) = graph.publication_facts();
        if root_inode != 1
            || highest_inode != 1
            || graph.counts().objects != record.object_count
            || graph.counts().canonical_inodes != 0
            || graph.counts().aliases != 0
            || graph.counts().groups != 0
            || graph.counts().directories != 1
            || graph.counts().source_records != 0
            || graph.counts().selector_records != 0
            || graph.counts().root_child_directories != 0
        {
            return Err(WorkspaceError::Fenced);
        }
        let root: InodeDelta = decode_open_value(&record.expected_root, REFERENCE_LIMIT)?;
        let snapshot = AuthenticatedV3Snapshot::open(client, manifest)
            .await
            .map_err(journal_budget_error)?;
        if snapshot
            .manifest()
            .source
            .as_ref()
            .map(|source| &source.root)
            != Some(&root_attributes(&root))
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(graph)
    }

    async fn bootstrap_initial_packed_owned<O: ObjectBackend + Clone + 'static>(
        self: &Arc<Self>,
        guard: HeadGuard,
        client: ObjectClient<O>,
        temporary: std::path::PathBuf,
        options: V3ProducerOptions,
        budget: Arc<V3MountBudget>,
        cancel: CancellationToken,
    ) -> Result<PackedLowerBindingRecord, WorkspaceError> {
        if cancel.is_cancelled() || budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        let mut record = self
            .begin_initial_bootstrap(&guard, &options, &budget)
            .await?;
        if record.phase == BootstrapPhase::Building {
            let root: InodeDelta = decode_open_value(&record.expected_root, REFERENCE_LIMIT)?;
            let _state_owner = budget
                .admit(&[(V3BudgetPool::Metadata, 64 << 10)])
                .map_err(journal_budget_error)?;
            let producer_owner = Arc::new(
                budget
                    .admit(&[(V3BudgetPool::Metadata, 4 << 20)])
                    .map_err(journal_budget_error)?,
            );
            let prefix = record.prefix();
            let state = Arc::new(tokio::sync::Mutex::new(record));
            let wrapped = client.clone().map_backend(|inner| InitialStagedBackend {
                inner,
                store: self.clone(),
                state: state.clone(),
                guard: guard.clone(),
                budget: budget.clone(),
            });
            let mut producer = V3SnapshotProducer::new_native(
                wrapped,
                &temporary,
                prefix,
                options,
                producer_owner,
                BOOTSTRAP_MAX_DISK,
            )
            .await
            .map_err(journal_budget_error)?;
            producer
                .set_root_attributes(root_attributes(&root))
                .map_err(journal_budget_error)?;
            let manifest = producer.finish().await.map_err(journal_budget_error)?;
            record = Arc::try_unwrap(state)
                .map_err(|_| journal_error("initial producer retained unfinished uploads"))?
                .into_inner();
            let mut candidate = record.next()?;
            candidate.phase = BootstrapPhase::Candidate;
            candidate.manifest = Some(manifest);
            self.bootstrap_write(&guard, &record, &candidate, &[], &budget)
                .await?;
            record = candidate;
        }
        if cancel.is_cancelled() || budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        let graph = self
            .audit_initial_bootstrap(&record, &client, &temporary, &budget, &cancel)
            .await?;
        let binding = if record.phase == BootstrapPhase::Installed {
            let binding = record.binding.clone().ok_or(WorkspaceError::Fenced)?;
            self.initial_packed_bootstrap_checks(&guard, &binding)
                .await?;
            binding
        } else {
            self.install_initial_bootstrap(&guard, &record, &graph, &budget, &cancel)
                .await?
        };
        // Registry activation still performs the complete existing bounded
        // current/history census. Bootstrap cannot fabricate an Active gate.
        self.migrate_packed_object_registry(
            &client,
            &budget,
            collector::PackedRegistryMigrationOptions {
                scratch: &temporary,
                graph_limits: V3IndexAuditLimits {
                    max_disk_bytes: BOOTSTRAP_MAX_DISK,
                    ..Default::default()
                },
                max_catalog_rows: 4096,
                cancel: cancel.clone(),
            },
        )
        .await?;
        Ok(binding)
    }

    async fn install_initial_bootstrap(
        &self,
        guard: &HeadGuard,
        record: &InitialBootstrapRecord,
        graph: &V3IndexContextAudit,
        budget: &Arc<V3MountBudget>,
        cancel: &CancellationToken,
    ) -> Result<PackedLowerBindingRecord, WorkspaceError> {
        if record.phase != BootstrapPhase::Candidate
            || cancel.is_cancelled()
            || budget.state().closed
        {
            return Err(WorkspaceError::Fenced);
        }
        let source = self
            .authenticate_initial_native_source(guard, Some(record), budget)
            .await?;
        let install = InstallPackedLowerBinding {
            guard: guard.clone(),
            expected_layers: [source.head.clone(), source.base.clone()],
            expected_base: revision_from_layer(&source.base)?,
            expected_binding: None,
            lower: VerifiedPackedLower::from_staged_graph(graph)?,
        };
        install.validate_native_root(&source.root)?;
        let binding = install.record()?;
        if Some(&binding.binding.manifest) != record.manifest.as_ref() || binding.highest_inode != 1
        {
            return Err(WorkspaceError::Fenced);
        }
        let root_key = registry_root_key(record.incarnation);
        let mapping_key = registry_history_root_key(&binding);
        let keys = [
            bootstrap_key(record.workspace_id),
            root_key.clone(),
            mapping_key.clone(),
            collector::GATE_KEY.to_vec(),
        ];
        let (values, _) = self.bootstrap_values(&keys).await?;
        if values[0].as_deref() != Some(record.encode()?.as_slice()) || values[2].is_some() {
            return Err(WorkspaceError::Fenced);
        }
        let mut root = RootRow::decode(values[1].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if root.journal_id != record.journal_id
            || root.incarnation != record.incarnation
            || root.state != RootState::Staging
            || root.members != record.object_count
            || root.members != graph.counts().objects
            || root.pending_puts != 0
            || root.binding.is_some()
        {
            return Err(WorkspaceError::Fenced);
        }
        root.state = RootState::BindingHistory;
        root.revision = increment(root.revision)?;
        root.binding = Some(binding.clone());
        let mut installed = record.next()?;
        installed.phase = BootstrapPhase::Installed;
        installed.binding = Some(binding.clone());
        let mut workspace = source.workspace.clone();
        workspace.head_epoch = binding.head_epoch;
        workspace.updated_at_ns = source.now;
        let mut head = source.head.clone();
        allocate_layer_sequences(&mut head, 1)?;
        let generation_raw = source
            .checks
            .iter()
            .find(|check| check.key == PACKED_ROOT_GENERATION_KEY)
            .ok_or(WorkspaceError::Fenced)?
            .expected
            .clone();
        let mut checks = source.checks.clone();
        merge(&mut checks, exact(&keys, &values))?;
        let binding_bytes = binding.encode()?;
        let root_bytes = root.encode()?;
        let install_endpoint = vec![
            KvWrite::Put {
                key: root_key,
                value: root_bytes.clone(),
            },
            KvWrite::Put {
                key: mapping_key,
                value: root_bytes,
            },
            KvWrite::Put {
                key: bootstrap_key(binding.workspace_id),
                value: installed.encode()?,
            },
        ];
        let mut writes = vec![
            put(hot_workspace_key(workspace.workspace_id), &workspace)?,
            put(hot_layer_key(head.layer_id), &head)?,
            put(hot_allocator_key("inode"), &2i64)?,
            KvWrite::Put {
                key: packed_current_key(binding.workspace_id),
                value: binding_bytes.clone(),
            },
            KvWrite::Put {
                key: packed_claim_key(binding.workspace_id),
                value: PACKED_CLAIM.to_vec(),
            },
            KvWrite::Put {
                key: packed_history_key(binding.workspace_id, 1),
                value: binding_bytes,
            },
            put(
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                &next_packed_root_generation(&generation_raw)?,
            )?,
        ];
        // Adopt the already authenticated initial graph in this same install
        // CAS. Existing history retirement can then use its genuine adopted
        // source path without requiring a fabricated PPJ Committed record.
        for ordinal in 0..record.object_count {
            let (occurrences, _) = self
                .bootstrap_values(&[object_key(record.journal_id, ordinal)])
                .await?;
            let occurrence = PackedJournalObject::decode(
                occurrences[0].as_deref().ok_or(WorkspaceError::Fenced)?,
            )?;
            let (member_checks, member) = self
                .bootstrap_registered_member(record, &occurrence.reference)
                .await?;
            let mut member = member.ok_or(WorkspaceError::Fenced)?;
            if member.ordinal != ordinal || member.pending_put || !member.dispatched {
                return Err(WorkspaceError::Fenced);
            }
            member.adopted = true;
            member.put_id = Uuid::nil();
            member.dispatched = false;
            merge(&mut checks, member_checks)?;
            writes.push(KvWrite::Put {
                key: registry_member_key(&occurrence.reference, record.incarnation),
                value: member.encode()?,
            });
        }
        writes.extend(install_endpoint);
        let _writer_owner = self
            .prepare_initial_packed_writer_authority(&binding, guard, &mut checks, &mut writes)
            .await?;
        let _native_holds = self
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await?;
        let packet = self
            .prepare_topology_envelope(checks, writes, Some(source.lease.expires_at_ns))
            .await?;
        if cancel.is_cancelled() || budget.state().closed {
            return Err(WorkspaceError::Busy);
        }
        // One actual mutation. Unknown replies retain the durable installed or
        // candidate endpoint; a fresh reopen authenticates and audits it again.
        if !self.commit_prepared_topology_packet(&packet).await? {
            return Err(WorkspaceError::Busy);
        }
        Ok(binding)
    }

    /// Prepare a provenance change; the private caller merges the real source,
    /// open and lease checks and writes it in their one actual claim CAS.
    pub(crate) async fn prepare_initial_bootstrap_claim(
        &self,
        binding: &PackedLowerBindingRecord,
        previous: Option<&[u8]>,
        next: Vec<u8>,
    ) -> Result<(KvCheck, KvWrite), WorkspaceError> {
        if next.is_empty() || next.len() > 4096 {
            return Err(WorkspaceError::Fenced);
        }
        let key = bootstrap_key(binding.workspace_id);
        let (values, _) = self.bootstrap_values(std::slice::from_ref(&key)).await?;
        let mut record =
            InitialBootstrapRecord::decode(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if record.phase != BootstrapPhase::Installed
            || record.binding.as_ref() != Some(binding)
            || record.first_admin_claim.as_deref() != previous
        {
            return Err(WorkspaceError::Fenced);
        }
        record = record.next()?;
        record.first_admin_claim = Some(next);
        Ok((
            KvCheck {
                key: key.clone(),
                expected: values[0].clone(),
            },
            KvWrite::Put {
                key,
                value: record.encode()?,
            },
        ))
    }

    /// Decode routing facts only. Typed source issuance checks the whole genuine
    /// retained PBI and current source; these bytes grant no authority themselves.
    pub(crate) async fn read_initial_bootstrap_claim(
        &self,
        workspace: WorkspaceId,
    ) -> Result<Option<Vec<u8>>, WorkspaceError> {
        let key = bootstrap_key(workspace);
        let (values, _) = self.bootstrap_values(std::slice::from_ref(&key)).await?;
        let record =
            InitialBootstrapRecord::decode(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if record.phase != BootstrapPhase::Installed || record.workspace_id != workspace {
            return Err(WorkspaceError::Fenced);
        }
        Ok(record.first_admin_claim)
    }

    /// Re-read the genuine retained first installation after native rotation.
    /// These facts grant no mutable head, open, lease, drain or phase authority.
    pub(crate) async fn retained_initial_packed_bootstrap_checks(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<(PackedLowerBindingRecord, Vec<KvCheck>), WorkspaceError> {
        let budget = self
            .packed_reader_pin_budget
            .get()
            .ok_or(WorkspaceError::Fenced)?;
        let _owner = budget
            .admit(&[(V3BudgetPool::Metadata, BOOTSTRAP_BYTES)])
            .map_err(journal_budget_error)?;
        let pbi_key = bootstrap_key(workspace_id);
        let (routing, _) = self
            .bootstrap_values(std::slice::from_ref(&pbi_key))
            .await?;
        let record =
            InitialBootstrapRecord::decode(routing[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if record.phase != BootstrapPhase::Installed || record.workspace_id != workspace_id {
            return Err(WorkspaceError::Fenced);
        }
        let binding = record
            .binding
            .as_ref()
            .ok_or(WorkspaceError::Fenced)?
            .clone();
        let keys = [
            pbi_key,
            packed_history_key(workspace_id, 1),
            registry_history_root_key(&binding),
            registry_root_key(record.incarnation),
            hot_layer_key(binding.base_revision.layer_id),
            inode_identity_key(binding.base_revision.layer_id, 1),
        ];
        let (values, now) = self.bootstrap_values(&keys).await?;
        if values[0] != routing[0]
            || values[1].as_deref() != Some(binding.encode()?.as_slice())
            || values[2] != values[3]
            || values[4].as_deref() != Some(record.expected_base.as_slice())
            || values[5].as_deref() != Some(record.expected_root.as_slice())
        {
            return Err(WorkspaceError::Fenced);
        }
        let root = RootRow::decode(values[2].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if root.state != RootState::BindingHistory
            || root.binding.as_ref() != Some(&binding)
            || root.journal_id != record.journal_id
            || root.incarnation != record.incarnation
            || root.members != record.object_count
            || root.pending_puts != 0
        {
            return Err(WorkspaceError::Fenced);
        }
        let checks = exact(&keys, &values);
        if !self
            .backend
            .compare_and_swap_before(&checks, &[], checked_expiry(now, 30_000_000_000)?)
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok((binding, checks))
    }

    /// Emit only immutable installed-source checks for native bootstrap phases.
    /// Live head/current/lease/open are authenticated in this issuance CAS and
    /// remain the native fence's responsibility across planned transitions.
    pub(crate) async fn initial_packed_bootstrap_checks(
        &self,
        guard: &HeadGuard,
        binding: &PackedLowerBindingRecord,
    ) -> Result<Vec<KvCheck>, WorkspaceError> {
        let budget =
            self.packed_reader_pin_budget
                .get()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "canonical registry budget",
                ))?;
        let _owner = budget
            .admit(&[(V3BudgetPool::Metadata, BOOTSTRAP_BYTES)])
            .map_err(journal_budget_error)?;
        let keys = [
            bootstrap_key(binding.workspace_id),
            packed_history_key(binding.workspace_id, 1),
            registry_history_root_key(binding),
            hot_layer_key(binding.base_revision.layer_id),
            inode_identity_key(binding.base_revision.layer_id, 1),
        ];
        let (values, _) = self.bootstrap_values(&keys).await?;
        let record =
            InitialBootstrapRecord::decode(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if record.phase != BootstrapPhase::Installed
            || record.binding.as_ref() != Some(binding)
            || values[1].as_deref() != Some(binding.encode()?.as_slice())
            || values[3].as_deref() != Some(record.expected_base.as_slice())
            || values[4].as_deref() != Some(record.expected_root.as_slice())
        {
            return Err(WorkspaceError::Fenced);
        }
        let root = RootRow::decode(values[2].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        if root.state != RootState::BindingHistory
            || root.binding.as_ref() != Some(binding)
            || root.journal_id != record.journal_id
            || root.incarnation != record.incarnation
            || root.members != record.object_count
            || root.pending_puts != 0
        {
            return Err(WorkspaceError::Fenced);
        }
        let root_key = registry_root_key(record.incarnation);
        let (actual, _) = self
            .bootstrap_values(std::slice::from_ref(&root_key))
            .await?;
        if actual[0] != values[2] {
            return Err(WorkspaceError::Fenced);
        }
        let mut immutable = exact(&keys, &values);
        merge(
            &mut immutable,
            exact(std::slice::from_ref(&root_key), &actual),
        )?;
        let mutable_keys = [
            hot_workspace_key(binding.workspace_id),
            hot_layer_key(binding.head_layer_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            packed_current_key(binding.workspace_id),
            packed_claim_key(binding.workspace_id),
            open_v3_recovery_key(binding.workspace_id),
        ];
        let (mutable, now) = self.bootstrap_values(&mutable_keys).await?;
        let workspace: WorkspaceRecord = decode_required(&mutable[0])?;
        let head: LayerRecord = decode_required(&mutable[1])?;
        let lease: SnapshotLease = decode_required(&mutable[2])?;
        let target_guard = HeadGuard {
            expected_head_epoch: binding.head_epoch,
            ..guard.clone()
        };
        checked_hot_guard(&workspace, &head, &lease, &target_guard, now)?;
        let mut expected_head: LayerRecord =
            decode_open_value(&record.expected_head, REFERENCE_LIMIT)?;
        allocate_layer_sequences(&mut expected_head, 1)?;
        if head != expected_head
            || mutable[3].as_deref() != Some(binding.encode()?.as_slice())
            || mutable[4].as_deref() != Some(PACKED_CLAIM)
            || mutable[5].is_some()
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut all = immutable.clone();
        merge(&mut all, exact(&mutable_keys, &mutable))?;
        let _writer_owner = self
            .authenticate_initial_packed_writer(guard, &mut all)
            .await?;
        if !self
            .backend
            .compare_and_swap_before(&all, &[], lease.expires_at_ns)
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(immutable)
    }
}

#[cfg(test)]
#[path = "initial_bootstrap_tests.rs"]
mod tests;
