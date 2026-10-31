//! Compose the genuine first packed-v3 installation with actual VFS drain,
//! native publication, transport teardown and one atomic root snapshot finish.
//! PBI/PBP are separate durable facts; neither impersonates clean-mount proof.

#[cfg(all(test, target_os = "linux"))]
#[path = "packed_initial_snapshot_tests.rs"]
mod tests;

use super::*;
use crate::workspace_overlay::stores::kv_store::packed_native_freeze::{
    NativePackedRecoveryClaimRequest, NativePrepareRecoveryRequest, PackedNativeQuiesceFence,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};

const INITIAL_OWNER_PREFIX: &str = "packed-v3-bootstrap/";
const INITIAL_MAGIC: &[u8; 5] = b"PBP3\x01";
const INITIAL_BYTES: u64 = 4 << 20;

#[cfg(test)]
fn initial_composer_diagnostic(stage: &str, error: &WorkspaceError) {
    let kind = match error {
        WorkspaceError::Fenced => "Fenced",
        WorkspaceError::Busy => "Busy",
        WorkspaceError::CorruptMetadata(_) => "CorruptMetadata",
        WorkspaceError::Backend(_) => "Backend",
        WorkspaceError::InvalidReadPlan(_) => "InvalidReadPlan",
        _ => "other",
    };
    eprintln!("[packed-v3-initial-composer-diag] stage={stage} error={kind}");
}

fn initial_published_key(workspace: WorkspaceId) -> Vec<u8> {
    format!("packed/v3/initial-snapshot/{workspace}").into_bytes()
}
fn initial_source_key(workspace: WorkspaceId) -> Vec<u8> {
    format!("packed/v3/initial-bootstrap/{workspace}").into_bytes()
}
fn initial_seed_key(journal: JournalId) -> Vec<u8> {
    format!("packed/v3/native-freeze-basis/{journal}").into_bytes()
}
fn initial_exact(keys: &[Vec<u8>], values: &[Option<Vec<u8>>]) -> Vec<KvCheck> {
    keys.iter()
        .cloned()
        .zip(values.iter().cloned())
        .map(|(key, expected)| KvCheck { key, expected })
        .collect()
}
fn initial_merge(
    checks: &mut Vec<KvCheck>,
    added: impl IntoIterator<Item = KvCheck>,
) -> Result<(), WorkspaceError> {
    for row in added {
        if let Some(old) = checks.iter().find(|old| old.key == row.key) {
            if old.expected != row.expected {
                return Err(WorkspaceError::Busy);
            }
        } else {
            checks.push(row);
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InitialOperationOrigin {
    snapshot_id: SnapshotId,
    native_journal_id: JournalId,
    planned_head: LayerId,
    fingerprint: [u8; 32],
}
impl InitialOperationOrigin {
    fn from_request(
        request: &PackedHeadlessSnapshotRequest,
        layout: ChunkLayout,
    ) -> Result<Self, WorkspaceError> {
        let mut hash = Sha256::new();
        hash.update(b"BrewFS packed-v3 initial logical request\0");
        for value in [
            request.snapshot_id.as_uuid(),
            request.native_journal_id.as_uuid(),
            request.new_head_layer_id.as_uuid(),
        ] {
            if value.is_nil() {
                return Err(WorkspaceError::Fenced);
            }
            hash.update(value.as_bytes());
        }
        for value in [&request.snapshot_name, &request.owner_id] {
            match value {
                None => hash.update([0]),
                Some(value) => {
                    if value.len() > 1024 {
                        return Err(WorkspaceError::Fenced);
                    }
                    hash.update([1]);
                    hash.update((value.len() as u64).to_le_bytes());
                    hash.update(value.as_bytes());
                }
            }
        }
        hash.update(request.producer.snapshot_id);
        hash.update(request.producer.root_dir_key);
        for value in [
            layout.chunk_size,
            u64::from(layout.block_size),
            request.producer.root_inode,
            request.producer.size_classes.min_frame_raw_bytes,
            request.producer.size_classes.max_random_frame_raw_bytes,
            request.producer.size_classes.max_sequential_frame_raw_bytes,
        ] {
            hash.update(value.to_le_bytes());
        }
        hash.update([
            request.producer.profile as u8,
            request.producer.build_policy.frames as u8,
            u8::from(request.producer.build_policy.inline_data),
            request.producer.metadata_codec as u8,
            request.producer.data_codec as u8,
        ]);
        Ok(Self {
            snapshot_id: request.snapshot_id,
            native_journal_id: request.native_journal_id,
            planned_head: request.new_head_layer_id,
            fingerprint: hash.finalize().into(),
        })
    }
    fn owner_id(&self) -> String {
        format!(
            "{INITIAL_OWNER_PREFIX}{}/{}/{}/{}",
            URL_SAFE_NO_PAD.encode(self.snapshot_id.as_bytes()),
            URL_SAFE_NO_PAD.encode(self.native_journal_id.as_bytes()),
            URL_SAFE_NO_PAD.encode(self.planned_head.as_bytes()),
            hex::encode(self.fingerprint)
        )
    }
    fn parse(raw: &str) -> Result<Option<Self>, WorkspaceError> {
        if !raw.starts_with(INITIAL_OWNER_PREFIX) {
            return Ok(None);
        }
        let fields = raw[INITIAL_OWNER_PREFIX.len()..]
            .split('/')
            .collect::<Vec<_>>();
        if fields.len() != 4 || raw.len() > OPEN_OWNER_MAX_BYTES {
            return Err(WorkspaceError::Fenced);
        }
        fn id(raw: &str) -> Result<uuid::Uuid, WorkspaceError> {
            let mut bytes = [0; 16];
            if raw.len() != 22
                || URL_SAFE_NO_PAD
                    .decode_slice(raw, &mut bytes)
                    .map_err(|_| WorkspaceError::Fenced)?
                    != 16
            {
                return Err(WorkspaceError::Fenced);
            }
            let id = uuid::Uuid::from_bytes(bytes);
            if id.is_nil() || URL_SAFE_NO_PAD.encode(bytes) != raw {
                return Err(WorkspaceError::Fenced);
            }
            Ok(id)
        }
        let mut fingerprint = [0; 32];
        hex::decode_to_slice(fields[3], &mut fingerprint).map_err(|_| WorkspaceError::Fenced)?;
        let value = Self {
            snapshot_id: SnapshotId::from_uuid(id(fields[0])?),
            native_journal_id: JournalId::from_uuid(id(fields[1])?),
            planned_head: LayerId::from_uuid(id(fields[2])?),
            fingerprint,
        };
        if value.owner_id() != raw {
            return Err(WorkspaceError::Fenced);
        }
        Ok(Some(value))
    }
    fn matches_request(
        &self,
        request: &PackedHeadlessSnapshotRequest,
        layout: ChunkLayout,
    ) -> Result<(), WorkspaceError> {
        if *self != Self::from_request(request, layout)? {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishedInitialReceipt {
    packed_journal_id: JournalId,
    snapshot: SnapshotRecord,
    initial_binding: Vec<u8>,
    binding: Vec<u8>,
    native_sealed_source_revision: BaseRevision,
    released_lease: SnapshotLease,
    closed_open: V3OpenRecord,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitialAdminClaim {
    original_guard: CleanReleaseGuard,
    original_open: V3OpenRecord,
    source_guard: CleanReleaseGuard,
    source_open: V3OpenRecord,
}
impl InitialAdminClaim {
    fn encode(&self) -> Result<Vec<u8>, WorkspaceError> {
        let original = self.original_guard.to_head_guard();
        let source = self.source_guard.to_head_guard();
        validate_open_record(&self.original_open, original.workspace_id)?;
        validate_open_record(&self.source_open, source.workspace_id)?;
        if original.workspace_id != source.workspace_id
            || original.expected_head_layer_id != source.expected_head_layer_id
            || original.expected_head_epoch != 1
            || source.expected_head_epoch != 1
            || source.holder_generation < original.holder_generation
            || (source.holder_generation == original.holder_generation
                && source.lease_id != original.lease_id)
            || self.original_open.owner_id != self.source_open.owner_id
            || self.source_open.generation < self.original_open.generation
            || self.original_open.state != V3OpenState::Ready
            || self.original_open.recovery_required
            || self.source_open.state != V3OpenState::Ready
            || self.source_open.recovery_required
            || InitialOperationOrigin::parse(&self.original_open.owner_id)?.is_none()
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut bytes = b"IBP3\x01".to_vec();
        bytes.extend(serde_json::to_vec(self).map_err(|_| WorkspaceError::Fenced)?);
        if bytes.len() > 4096 {
            return Err(WorkspaceError::Fenced);
        }
        Ok(bytes)
    }
    fn decode(raw: &[u8]) -> Result<Self, WorkspaceError> {
        if raw.len() > 4096 || !raw.starts_with(b"IBP3\x01") {
            return Err(WorkspaceError::Fenced);
        }
        let value: Self = serde_json::from_slice(&raw[5..]).map_err(|_| WorkspaceError::Fenced)?;
        if value.encode()? != raw {
            return Err(WorkspaceError::Fenced);
        }
        Ok(value)
    }
}
fn encode_initial_receipt(value: &PublishedInitialReceipt) -> Result<Vec<u8>, WorkspaceError> {
    let mut bytes = INITIAL_MAGIC.to_vec();
    bytes.extend(serde_json::to_vec(value).map_err(|_| WorkspaceError::Fenced)?);
    if bytes.len() > 16 << 10 {
        return Err(WorkspaceError::Fenced);
    }
    Ok(bytes)
}
fn decode_initial_receipt(bytes: &[u8]) -> Result<PublishedInitialReceipt, WorkspaceError> {
    if bytes.len() > 16 << 10 || !bytes.starts_with(INITIAL_MAGIC) {
        return Err(WorkspaceError::Fenced);
    }
    let value = serde_json::from_slice(&bytes[INITIAL_MAGIC.len()..])
        .map_err(|_| WorkspaceError::Fenced)?;
    if encode_initial_receipt(&value)? != bytes {
        return Err(WorkspaceError::Fenced);
    }
    Ok(value)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum InitialAuthorityMode {
    Original,
    Recovery,
    Committed,
}

/// Issued only from the retained genuine PBI installation and an actual
/// current open/lease incarnation. No public constructor or decoded ticket.
pub(crate) struct PackedInitialBootstrapAuthority<B> {
    store: Arc<KvWorkspaceStore<B>>,
    initial_binding: PackedLowerBindingRecord,
    immutable: Vec<KvCheck>,
    open: V3OpenRecord,
    guard: HeadGuard,
    origin: InitialOperationOrigin,
    budget: Arc<V3MountBudget>,
    mode: InitialAuthorityMode,
    _owner: V3OwnedPermit,
}
impl<B: WorkspaceKvBackend> PackedInitialBootstrapAuthority<B> {
    pub(crate) fn belongs_to_store(&self, store: &Arc<KvWorkspaceStore<B>>) -> bool {
        Arc::ptr_eq(&self.store, store)
    }
    pub(crate) fn mount_budget(&self) -> &Arc<V3MountBudget> {
        &self.budget
    }
    pub(crate) fn guard(&self) -> &HeadGuard {
        &self.guard
    }
    pub(crate) async fn authority_checks_before(
        &self,
    ) -> Result<(Vec<KvCheck>, i64), WorkspaceError> {
        if self.budget.state().closed || self.mode == InitialAuthorityMode::Committed {
            #[cfg(test)]
            eprintln!(
                "[packed-v3-initial-composer-diag] stage=authority-admission budget_closed={} committed_mode={}",
                self.budget.state().closed,
                self.mode == InitialAuthorityMode::Committed
            );
            return Err(WorkspaceError::Fenced);
        }
        let keys = [
            open_v3_key(self.guard.workspace_id),
            hot_lease_key(self.guard.workspace_id, self.guard.lease_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            CONTROL_KEY.to_vec(),
            hot_workspace_key(self.guard.workspace_id),
            lease_index_key(self.guard.lease_id),
        ];
        let (values, now) = self
            .store
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let open: V3OpenRecord = decode_open_value(
            values[0].as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        let lease: SnapshotLease = decode_open_value(
            values[1].as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        validate_current_control_raw(values[4].as_deref())?;
        let workspace: WorkspaceRecord = decode_open_value(
            values[5].as_deref().ok_or(WorkspaceError::Fenced)?,
            SOURCE_MAX_BYTES,
        )?;
        let lease_workspace: WorkspaceId = decode_open_value(
            values[6].as_deref().ok_or(WorkspaceError::Fenced)?,
            SOURCE_MAX_BYTES,
        )?;
        validate_open_record(&open, self.guard.workspace_id)?;
        if open != self.open
            || open.expires_at_ns <= now
            || open.owner_id != self.origin.owner_id()
            || match self.mode {
                InitialAuthorityMode::Original => {
                    open.state != V3OpenState::Ready || open.recovery_required
                }
                InitialAuthorityMode::Recovery => {
                    open.state != V3OpenState::Recovering || !open.recovery_required
                }
                InitialAuthorityMode::Committed => true,
            }
            || lease.lease_id != self.guard.lease_id
            || lease.workspace_id != self.guard.workspace_id
            || lease.holder_generation != self.guard.holder_generation
            || lease.base_revision != self.initial_binding.base_revision
            || workspace.workspace_id != self.guard.workspace_id
            || workspace.active_lease != Some(lease.lease_id)
            || lease_workspace != self.guard.workspace_id
            || !lease.writable
            || lease.state != LeaseState::Active
            || lease.expires_at_ns <= now
        {
            #[cfg(test)]
            eprintln!(
                "[packed-v3-initial-composer-diag] stage=authority-open-lease open_matches={} open_live={} owner_matches={} original_mode={} ready={} recovering={} recovery_required={} lease_identity={} lease_generation={} writable={} active={} lease_live={}",
                open == self.open,
                open.expires_at_ns > now,
                open.owner_id == self.origin.owner_id(),
                self.mode == InitialAuthorityMode::Original,
                open.state == V3OpenState::Ready,
                open.state == V3OpenState::Recovering,
                open.recovery_required,
                lease.lease_id == self.guard.lease_id
                    && lease.workspace_id == self.guard.workspace_id,
                lease.holder_generation == self.guard.holder_generation,
                lease.writable,
                lease.state == LeaseState::Active,
                lease.expires_at_ns > now
            );
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[2])?;
        layer_inventory_generation(&values[3])?;
        let mut checks = initial_exact(&keys, &values);
        let immutable_keys = self
            .immutable
            .iter()
            .map(|row| row.key.clone())
            .collect::<Vec<_>>();
        let (actual, _) = self
            .store
            .backend
            .get_many_consistent_with_time_bounded(
                &immutable_keys,
                source_limits(immutable_keys.len()),
            )
            .await?;
        if actual.len() != self.immutable.len()
            || actual
                .iter()
                .zip(&self.immutable)
                .any(|(value, row)| value != &row.expected)
        {
            #[cfg(test)]
            eprintln!(
                "[packed-v3-initial-composer-diag] stage=authority-immutable count_matches={} values_match={}",
                actual.len() == self.immutable.len(),
                actual
                    .iter()
                    .zip(&self.immutable)
                    .all(|(value, row)| value == &row.expected)
            );
            return Err(WorkspaceError::Fenced);
        }
        initial_merge(&mut checks, self.immutable.clone())?;
        let _writer_owner = self
            .store
            .authenticate_administrative_packed_writer(self.guard.workspace_id, &mut checks)
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                initial_composer_diagnostic("authority-administrative-writer", _error);
            })?;
        Ok((checks, open.expires_at_ns.min(lease.expires_at_ns)))
    }
}

struct InitialRuntimeOwner<B: WorkspaceKvBackend, S: BlockStore + Send + Sync + 'static> {
    authority: Arc<PackedInitialBootstrapAuthority<B>>,
    store: Arc<KvWorkspaceStore<B>>,
    budget: Arc<V3MountBudget>,
    upper: Arc<S>,
    temporary_owner: Option<Arc<tempfile::TempDir>>,
    cleanup_handle: tokio::runtime::Handle,
    metadata: std::sync::Mutex<Option<Arc<WorkspaceMetaLayer<KvWorkspaceStore<B>>>>>,
    reader: std::sync::Mutex<
        Option<Arc<dyn crate::workspace_overlay::packed_reader_lifecycle::PackedReaderSession>>,
    >,
    native: std::sync::Mutex<Option<Arc<PackedNativeQuiesceFence<B>>>>,
    finished: std::sync::atomic::AtomicBool,
}
impl<B: WorkspaceKvBackend, S: BlockStore + Send + Sync + 'static> Drop
    for InitialRuntimeOwner<B, S>
{
    fn drop(&mut self) {
        if self.finished.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let authority = self.authority.clone();
        let store = self.store.clone();
        let upper = self.upper.clone();
        let temporary_owner = self.temporary_owner.clone();
        let budget = self.budget.clone();
        let metadata = self.metadata.get_mut().unwrap().take();
        let reader = self.reader.get_mut().unwrap().take();
        let native = self.native.get_mut().unwrap().take();
        self.cleanup_handle.spawn(async move {
            let _authority = authority;
            let _store = store;
            let _upper = upper;
            let _temporary_owner = temporary_owner;
            let _native = native;
            if let Some(metadata) = metadata {
                if metadata
                    .shutdown_packed_runtime_for_clean_release()
                    .await
                    .is_ok()
                {
                    budget.close();
                }
            } else if let Some(reader) = reader {
                if reader.shutdown().await.is_ok() {
                    budget.close();
                }
            } else {
                budget.close();
            }
            // Durable PBI/open/lease/NQB/PPJ remain real recovery obligations.
        });
    }
}
fn initial_failure<B: WorkspaceKvBackend, S: BlockStore + Send + Sync + 'static>(
    error: WorkspaceError,
    runtime: &Arc<InitialRuntimeOwner<B, S>>,
) -> PackedHeadlessSnapshotFailure {
    PackedHeadlessSnapshotFailure::held((error, runtime.clone()), |owner| &owner.0)
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    async fn initial_topology_view(
        &self,
        keys: &[Vec<u8>],
        workspace: WorkspaceId,
        leases: &[LeaseId],
        journals: &[JournalId],
        history: bool,
    ) -> Result<(ScopedTopology, Vec<Option<Vec<u8>>>), WorkspaceError> {
        let history = if history {
            self.read_workspace_lease_history_checks(workspace, 16, source_limits(32))
                .await?
        } else {
            Vec::new()
        };
        let mut extra_keys = keys.to_vec();
        extra_keys.extend(history.iter().map(|check| check.key.clone()));
        let scope = TopologyScope {
            workspaces: vec![workspace],
            leases: leases.iter().map(|id| (workspace, *id)).collect(),
            journals: journals.iter().map(|id| (workspace, *id)).collect(),
            extra_keys,
            workspace_heads: true,
            ..TopologyScope::default()
        };
        let mut basis = self.read_topology_scope(&scope, source_limits(32)).await?;
        initial_merge(&mut basis.checks, history)?;
        let values = keys
            .iter()
            .map(|key| {
                basis
                    .checks
                    .iter()
                    .find(|check| &check.key == key)
                    .map(|check| check.expected.clone())
                    .ok_or(WorkspaceError::Fenced)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((basis, values))
    }

    /// Create only the actual fresh native root catalog; no native Snapshot.
    /// The complete bounded predecessor and native inventory share one CAS.
    async fn ensure_initial_volume_root(
        &self,
        request: &CreateVolumeRoot,
    ) -> Result<(), WorkspaceError> {
        if request.schema_version != WORKSPACE_SCHEMA_VERSION
            || request.volume_id.is_nil()
            || request.root_layer_id == request.writable_layer_id
        {
            return Err(WorkspaceError::Fenced);
        }
        self.initialize_workspace_schema().await?;
        let keys = vec![
            CONTROL_KEY.to_vec(),
            VOLUME_HEADER_KEY.to_vec(),
            hot_workspace_key(request.workspace_id),
            hot_layer_key(request.root_layer_id),
            hot_layer_key(request.writable_layer_id),
            inode_identity_key(request.root_layer_id, 1),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            hot_allocator_key("inode"),
            hot_allocator_key("slice"),
            hot_allocator_key("sealed_version"),
        ];
        let (basis, values) = self
            .initial_topology_view(&keys, request.workspace_id, &[], &[], false)
            .await?;
        let now = basis.now_ns;
        if values.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        if let Some(raw) = values[1].as_deref() {
            let header: VolumeHeader = decode_open_value(raw, SOURCE_MAX_BYTES)?;
            let workspace: WorkspaceRecord = decode_open_value(
                values[2].as_deref().ok_or(WorkspaceError::Fenced)?,
                SOURCE_MAX_BYTES,
            )?;
            if header.volume_id != request.volume_id
                || header.volume_format != request.volume_format
                || header.schema_version != request.schema_version
                || workspace.workspace_id != request.workspace_id
                || workspace.owner_id != request.owner_id
            {
                return Err(WorkspaceError::Fenced);
            }
            if !self
                .backend
                .compare_and_swap_before(
                    &initial_exact(&keys, &values),
                    &[],
                    checked_expiry(now, 30_000_000_000)?,
                )
                .await?
            {
                return Err(WorkspaceError::Busy);
            }
            return Ok(());
        }
        let before = basis.state.clone();
        if before.schema_version != WORKSPACE_SCHEMA_VERSION
            || before.header.is_some()
            || !before.workspaces.is_empty()
            || !before.layers.is_empty()
            || !before.leases.is_empty()
            || !before.journals.is_empty()
            || !before.snapshots.is_empty()
            || values[2..6].iter().any(Option::is_some)
            || values[7..].iter().any(Option::is_some)
        {
            return Err(WorkspaceError::Fenced);
        }
        let root_inode = InodeDelta {
            layer_id: request.root_layer_id,
            ino: 1,
            state: InodeState::Present,
            kind: 1,
            size: 0,
            mode: 0o755,
            uid: 0,
            gid: 0,
            rdev: 0,
            nlink: 2,
            atime_ns: now,
            mtime_ns: now,
            ctime_ns: now,
            symlink_target: None,
            parent_hint: Some(1),
            data_version: 1,
            sequence: 1,
        };
        let digest = delta_digest(&CanonicalLayerDelta {
            inodes: vec![root_inode.clone()],
            ..CanonicalLayerDelta::default()
        })?;
        let root = root_hash([0; 32], digest);
        let base = LayerRecord {
            layer_id: request.root_layer_id,
            parent_layer_id: None,
            state: LayerState::Sealed,
            schema_version: WORKSPACE_SCHEMA_VERSION,
            sealed_version: Some(1),
            delta_digest: Some(digest),
            root_hash: Some(root),
            depth: 1,
            owner_workspace_id: None,
            next_sequence: 2,
            owned_slice_count: 0,
            owned_bytes: 0,
            created_at_ns: now,
            sealed_at_ns: Some(now),
        };
        let workspace = WorkspaceRecord {
            workspace_id: request.workspace_id,
            head_layer_id: request.writable_layer_id,
            head_epoch: 0,
            fork_base: Some(BaseRevision {
                layer_id: base.layer_id,
                sealed_version: 1,
                root_hash: root,
            }),
            owner_id: request.owner_id.clone(),
            upper_bucket: request.upper_bucket.clone(),
            state: WorkspaceState::Active,
            active_lease: None,
            created_at_ns: now,
            updated_at_ns: now,
        };
        let header = VolumeHeader {
            volume_format: request.volume_format.clone(),
            schema_version: request.schema_version,
            volume_id: request.volume_id,
            created_at_ns: now,
        };
        let mut after = before.clone();
        after.header = Some(header.clone());
        after.layers.insert(base.layer_id, base);
        after.layers.insert(
            request.writable_layer_id,
            writable_layer(
                request.writable_layer_id,
                request.root_layer_id,
                2,
                request.workspace_id,
                now,
            ),
        );
        after.workspaces.insert(workspace.workspace_id, workspace);
        after.allocators.insert("inode".into(), 2);
        after.allocators.insert("slice".into(), 1);
        after.allocators.insert("sealed_version".into(), 2);
        let mut checks = basis.checks.clone();
        let mut writes = vec![
            put(VOLUME_HEADER_KEY.to_vec(), &header)?,
            put(inode_key(&root_inode), &root_inode)?,
            put(
                LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                &next_layer_inventory_generation(&values[6])?,
            )?,
        ];
        self.stage_topology_diff(&basis, &after, &mut checks, &mut writes)?;
        let _native_reverse = self
            .prepare_native_reverse_cas(&mut checks, &mut writes)
            .await?;
        let _holds = self
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await?;
        self.clean_exact_cas(&checks, &writes, checked_expiry(now, 30_000_000_000)?)
            .await
    }

    /// Grant or re-read an existing first admin lease before an open or NQB.
    /// A missing root snapshot never permits replacing a later native source.
    async fn initial_lease_before_install(
        &self,
        volume: &CreateVolumeRoot,
        request: &PackedHeadlessSnapshotRequest,
    ) -> Result<HeadGuard, WorkspaceError> {
        let keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(volume.workspace_id),
            hot_layer_key(volume.writable_layer_id),
            hot_layer_key(volume.root_layer_id),
            hot_lease_key(volume.workspace_id, request.lease_id),
            open_v3_key(volume.workspace_id),
            open_v3_recovery_key(volume.workspace_id),
            initial_seed_key(request.native_journal_id),
            hot_snapshot_key(request.snapshot_id),
            initial_published_key(volume.workspace_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            packed_current_key(volume.workspace_id),
            packed_claim_key(volume.workspace_id),
            packed_history_key(volume.workspace_id, 1),
            crate::workspace_overlay::stores::kv_store::packed_writer_authority::packed_writer_key(
                volume.workspace_id,
            ),
        ];
        let (basis, values) = self
            .initial_topology_view(
                &keys,
                volume.workspace_id,
                &[request.lease_id],
                &[request.native_journal_id],
                true,
            )
            .await?;
        let now = basis.now_ns;
        if values.len() != keys.len()
            || now <= 0
            || values[5..10].iter().any(Option::is_some)
            || values[12..].iter().any(Option::is_some)
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut control = basis.state.clone();
        let workspace: WorkspaceRecord = decode_open_value(
            values[1].as_deref().ok_or(WorkspaceError::Fenced)?,
            SOURCE_MAX_BYTES,
        )?;
        let head: LayerRecord = decode_open_value(
            values[2].as_deref().ok_or(WorkspaceError::Fenced)?,
            SOURCE_MAX_BYTES,
        )?;
        let base: LayerRecord = decode_open_value(
            values[3].as_deref().ok_or(WorkspaceError::Fenced)?,
            SOURCE_MAX_BYTES,
        )?;
        next_packed_root_generation(&values[10])?;
        layer_inventory_generation(&values[11])?;
        if control.schema_version != WORKSPACE_SCHEMA_VERSION
            || control.workspaces.get(&workspace.workspace_id) != Some(&workspace)
            || control.layers.get(&head.layer_id) != Some(&head)
            || control.layers.get(&base.layer_id) != Some(&base)
            || workspace.workspace_id != volume.workspace_id
            || workspace.head_layer_id != volume.writable_layer_id
            || workspace.head_epoch > 1
            || workspace.state != WorkspaceState::Active
            || head.layer_id != volume.writable_layer_id
            || head.state != LayerState::Writable
            || head.parent_layer_id != Some(volume.root_layer_id)
            || head.owner_workspace_id != Some(volume.workspace_id)
            || head.depth != 2
            || base.layer_id != volume.root_layer_id
            || base.state != LayerState::Sealed
            || base.depth != 1
            || base.parent_layer_id.is_some()
            || workspace_has_incomplete_seal(&control, volume.workspace_id)
        {
            return Err(WorkspaceError::Fenced);
        }
        let revision = BaseRevision {
            layer_id: base.layer_id,
            sealed_version: base.sealed_version.ok_or(WorkspaceError::Fenced)?,
            root_hash: base.root_hash.ok_or(WorkspaceError::Fenced)?,
        };
        let mut checks = basis.checks.clone();
        let existing: Option<SnapshotLease> = values[4]
            .as_deref()
            .map(|raw| decode_open_value(raw, SOURCE_MAX_BYTES))
            .transpose()?;
        if let Some(lease) = existing {
            let guard = HeadGuard {
                workspace_id: workspace.workspace_id,
                expected_head_layer_id: head.layer_id,
                expected_head_epoch: workspace.head_epoch,
                lease_id: lease.lease_id,
                holder_generation: lease.holder_generation,
            };
            checked_hot_guard(&workspace, &head, &lease, &guard, now)?;
            if workspace.active_lease != Some(lease.lease_id)
                || lease.base_revision != revision
                || control.leases.get(&lease.lease_id) != Some(&lease)
            {
                return Err(WorkspaceError::Fenced);
            }
            if !self
                .backend
                .compare_and_swap_before(&checks, &[], lease.expires_at_ns)
                .await?
            {
                return Err(WorkspaceError::Busy);
            }
            return Ok(guard);
        }
        if control.leases.values().any(|lease| {
            lease.workspace_id == volume.workspace_id
                && lease.writable
                && lease.state == LeaseState::Active
                && lease.expires_at_ns > now
        }) {
            return Err(WorkspaceError::Busy);
        }
        let generation = control
            .leases
            .values()
            .filter(|lease| lease.workspace_id == volume.workspace_id)
            .map(|lease| lease.holder_generation)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(WorkspaceError::Fenced)?;
        let lease = SnapshotLease {
            lease_id: request.lease_id,
            workspace_id: volume.workspace_id,
            base_revision: revision,
            holder_generation: generation,
            writable: true,
            state: LeaseState::Active,
            expires_at_ns: checked_expiry(now, request.lease_ttl_ns)?,
            created_at_ns: now,
            updated_at_ns: now,
        };
        for previous in control.leases.values_mut() {
            if previous.workspace_id == volume.workspace_id
                && previous.state == LeaseState::Active
                && previous.expires_at_ns <= now
            {
                previous.state = LeaseState::Expired;
                previous.updated_at_ns = now;
            }
        }
        control.leases.insert(lease.lease_id, lease.clone());
        let current = control
            .workspaces
            .get_mut(&volume.workspace_id)
            .ok_or(WorkspaceError::Fenced)?;
        current.active_lease = Some(lease.lease_id);
        current.updated_at_ns = now;
        let mut writes = Vec::new();
        self.stage_topology_diff(&basis, &control, &mut checks, &mut writes)?;
        for write in &writes {
            let key = match write {
                KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
            };
            if !checks.iter().any(|row| row.key == *key) {
                let (raw, _) = self
                    .backend
                    .get_many_consistent_with_time_bounded(
                        std::slice::from_ref(key),
                        source_limits(1),
                    )
                    .await?;
                if raw.len() != 1 {
                    return Err(WorkspaceError::Fenced);
                }
                checks.push(KvCheck {
                    key: key.clone(),
                    expected: raw[0].clone(),
                });
            }
        }
        let _holds = self
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await?;
        self.clean_exact_cas(&checks, &writes, lease.expires_at_ns)
            .await?;
        Ok(HeadGuard {
            workspace_id: volume.workspace_id,
            expected_head_layer_id: head.layer_id,
            expected_head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: generation,
        })
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    async fn issue_initial_authority(
        self: &Arc<Self>,
        guard: HeadGuard,
        mode: InitialAuthorityMode,
        budget: Arc<V3MountBudget>,
    ) -> Result<Arc<PackedInitialBootstrapAuthority<B>>, WorkspaceError> {
        let owner = budget
            .admit(&[(V3BudgetPool::Metadata, INITIAL_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let (initial_binding, immutable) = self
            .retained_initial_packed_bootstrap_checks(guard.workspace_id)
            .await?;
        let claim_raw = self
            .read_initial_bootstrap_claim(guard.workspace_id)
            .await?
            .ok_or(WorkspaceError::Fenced)?;
        let claim = InitialAdminClaim::decode(&claim_raw)?;
        if guard.expected_head_layer_id != initial_binding.head_layer_id
            || guard.expected_head_epoch != initial_binding.head_epoch
        {
            return Err(WorkspaceError::Fenced);
        }
        let keys = [
            open_v3_key(guard.workspace_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
        ];
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let open: V3OpenRecord = decode_open_value(
            values[0].as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        let lease: SnapshotLease = decode_open_value(
            values[1].as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        validate_open_record(&open, guard.workspace_id)?;
        let origin =
            InitialOperationOrigin::parse(&open.owner_id)?.ok_or(WorkspaceError::Fenced)?;
        let original_origin = InitialOperationOrigin::parse(&claim.original_open.owner_id)?
            .ok_or(WorkspaceError::Fenced)?;
        if open.expires_at_ns <= now
            || lease.expires_at_ns <= now
            || lease.lease_id != guard.lease_id
            || lease.workspace_id != guard.workspace_id
            || lease.holder_generation != guard.holder_generation
            || lease.state != LeaseState::Active
            || !lease.writable
            || lease.base_revision != initial_binding.base_revision
            || origin != original_origin
            || guard.holder_generation < claim.source_guard.holder_generation
            || (guard.holder_generation == claim.source_guard.holder_generation
                && guard.lease_id != claim.source_guard.lease_id)
            || (mode == InitialAuthorityMode::Original
                && (guard != claim.source_guard.to_head_guard() || open != claim.source_open))
        {
            return Err(WorkspaceError::Fenced);
        }
        let authority = Arc::new(PackedInitialBootstrapAuthority {
            store: self.clone(),
            initial_binding,
            immutable,
            open,
            guard,
            origin,
            budget,
            mode,
            _owner: owner,
        });
        let (checks, deadline) =
            authority
                .authority_checks_before()
                .await
                .inspect_err(|_error| {
                    #[cfg(test)]
                    initial_composer_diagnostic("issue-authority-checks", _error);
                })?;
        if !self
            .backend
            .compare_and_swap_before(&checks, &[], deadline)
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        Ok(authority)
    }

    /// Only the genuine PBI private claim may retain its actual original lease
    /// through Prepare/Q. The exact Recovering open fences the old publisher;
    /// all source provenance is consumed in the following actual seed CAS.
    pub(in crate::workspace_overlay::stores::kv_store) async fn retained_original_initial_seed_recovery_checks(
        self: &Arc<Self>,
        mapping: &crate::workspace_overlay::stores::kv_store::packed_native_freeze::PackedNativePlannedRotation,
        binding: &PackedLowerBindingRecord,
        head_sequence: u64,
        expected_open: &V3OpenRecord,
        expected_lease: &SnapshotLease,
        budget: &Arc<V3MountBudget>,
    ) -> Result<Option<(Vec<KvCheck>, Arc<PackedInitialBootstrapAuthority<B>>)>, WorkspaceError>
    {
        let guard = mapping.old_guard();
        let native_id = mapping.journal_id();
        let planned_head = mapping.planned_head_layer_id();
        let Some(origin) = InitialOperationOrigin::parse(&expected_open.owner_id)? else {
            return Ok(None);
        };
        if budget.state().closed
            || origin.native_journal_id != native_id
            || origin.planned_head != planned_head
            || head_sequence != 2
            || expected_lease.lease_id != guard.lease_id
            || expected_lease.workspace_id != guard.workspace_id
            || expected_lease.holder_generation != guard.holder_generation
        {
            return Err(WorkspaceError::Fenced);
        }
        let claim_raw = self
            .read_initial_bootstrap_claim(guard.workspace_id)
            .await?
            .ok_or(WorkspaceError::Fenced)?;
        let claim = InitialAdminClaim::decode(&claim_raw)?;
        if claim.source_guard.to_head_guard() != *guard
            || claim.source_open.workspace_id != expected_open.workspace_id
            || claim.source_open.owner_id != expected_open.owner_id
            || claim.source_open.generation != expected_open.generation
            || expected_open.state != V3OpenState::Recovering
            || !expected_open.recovery_required
        {
            return Err(WorkspaceError::Fenced);
        }
        let authority = self
            .issue_initial_authority(
                guard.clone(),
                InitialAuthorityMode::Recovery,
                budget.clone(),
            )
            .await?;
        if authority.initial_binding != *binding || authority.open != *expected_open {
            return Err(WorkspaceError::Fenced);
        }
        let (checks, _) = authority.authority_checks_before().await?;
        Ok(Some((checks, authority)))
    }

    /// Called after genuine NQB/PNB recovery supplies its actual current fence.
    /// A normal clean owner is ignored. An initial owner must authenticate PBI.
    pub(crate) async fn restore_initial_native_origin(
        self: &Arc<Self>,
        native: &PackedNativeQuiesceFence<B>,
    ) -> Result<Option<Arc<PackedInitialBootstrapAuthority<B>>>, WorkspaceError> {
        if !native.is_same_store(self) {
            return Err(WorkspaceError::Fenced);
        }
        let guard = native.source_guard().clone();
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                &[open_v3_key(guard.workspace_id)],
                source_limits(1),
            )
            .await?;
        if values.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let open: V3OpenRecord = decode_open_value(
            values[0].as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        let Some(origin) = InitialOperationOrigin::parse(&open.owner_id)? else {
            return Ok(None);
        };
        if (!native.requires_recovery_owner() && native.recovery_basis().is_none())
            || origin.native_journal_id != native.mapping().journal_id()
            || origin.planned_head != native.mapping().planned_head_layer_id()
        {
            return Err(WorkspaceError::Fenced);
        }
        let mode = if native.requires_recovery_owner() {
            InitialAuthorityMode::Recovery
        } else {
            InitialAuthorityMode::Original
        };
        let source = self
            .issue_initial_authority(guard, mode, native.mount_budget())
            .await?;
        let claim_raw = self
            .read_initial_bootstrap_claim(source.guard.workspace_id)
            .await?
            .ok_or(WorkspaceError::Fenced)?;
        let claim = InitialAdminClaim::decode(&claim_raw)?;
        if source.initial_binding != *native.binding()
            || native.mapping().old_guard().expected_head_layer_id
                != source.initial_binding.head_layer_id
            || native.mapping().old_guard().expected_head_epoch != source.initial_binding.head_epoch
            || native.mapping().old_layers()[0].next_sequence != 2
            || native.mapping().old_guard() != &claim.source_guard.to_head_guard()
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(Some(source))
    }

    async fn finish_initial_publication(
        &self,
        authority: &PackedInitialBootstrapAuthority<B>,
        result: &PackedSnapshotResult,
        record: &crate::workspace_overlay::stores::kv_store::packed_journal::PackedJournalRecord,
        name: Option<String>,
        owner_id: Option<String>,
    ) -> Result<(), WorkspaceError> {
        use crate::workspace_overlay::stores::kv_store::packed_journal::PackedJournalPhase;
        let (native_id, planned, source_version, carrier) =
            record.native_completion_identities()?;
        let (source_hash, _) = record
            .final_source_hash_facts()
            .ok_or(WorkspaceError::Fenced)?;
        let original_source_guard = record.final_source_guard().ok_or(WorkspaceError::Fenced)?;
        if !std::ptr::eq(self, authority.store.as_ref())
            || authority.budget.state().closed
            || result.packed_carrier_revision != result.binding.base_revision
            || result.binding.workspace_id != authority.guard.workspace_id
            || record.phase != PackedJournalPhase::Committed
            || record.expected_binding != authority.initial_binding
            || record.commit_target.as_ref() != Some(&result.binding)
            || native_id != authority.origin.native_journal_id
            || planned != authority.origin.planned_head
            || carrier != result.packed_carrier_revision
            || result.native_sealed_source_revision.layer_id != record.guard.expected_head_layer_id
            || result.native_sealed_source_revision.sealed_version != source_version
            || result.native_sealed_source_revision.root_hash != source_hash
            || original_source_guard.lease_id != authority.guard.lease_id
            || original_source_guard.holder_generation != authority.guard.holder_generation
            || result.snapshot_id != authority.origin.snapshot_id
        {
            return Err(WorkspaceError::Fenced);
        }
        let guard = HeadGuard {
            expected_head_layer_id: result.binding.head_layer_id,
            expected_head_epoch: result.binding.head_epoch,
            ..authority.guard.clone()
        };
        let mut keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(guard.workspace_id),
            hot_layer_key(guard.expected_head_layer_id),
            hot_layer_key(carrier.layer_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            packed_current_key(guard.workspace_id),
            packed_claim_key(guard.workspace_id),
            packed_history_key(guard.workspace_id, result.binding.binding.binding_version),
            open_v3_key(guard.workspace_id),
            open_v3_recovery_key(guard.workspace_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            hot_snapshot_key(result.snapshot_id),
            initial_published_key(guard.workspace_id),
            format!("packed/v3/journal/{}", record.journal_id).into_bytes(),
        ];
        if let Some(name) = &name {
            keys.push(snapshot_name_key(name));
        }
        let (basis, values) = self
            .initial_topology_view(
                &keys,
                guard.workspace_id,
                &[guard.lease_id],
                &[native_id],
                true,
            )
            .await?;
        let now = basis.now_ns;
        if values.len() != keys.len()
            || now <= 0
            || values[14].as_deref() != Some(record.encode()?.as_slice())
        {
            return Err(WorkspaceError::Fenced);
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        let mut control = basis.state.clone();
        let workspace: WorkspaceRecord = decode_open_value(required(1)?, SOURCE_MAX_BYTES)?;
        let head: LayerRecord = decode_open_value(required(2)?, SOURCE_MAX_BYTES)?;
        let base: LayerRecord = decode_open_value(required(3)?, SOURCE_MAX_BYTES)?;
        let mut lease: SnapshotLease = decode_open_value(required(4)?, SOURCE_MAX_BYTES)?;
        let current = decode_packed_pair(guard.workspace_id, &values[5], &values[6], &values[7])?
            .ok_or(WorkspaceError::Fenced)?;
        let mut open: V3OpenRecord = decode_open_value(required(8)?, OPEN_RECORD_MAX_BYTES)?;
        let recovery: Option<V3RecoveryRecord> = values[9]
            .as_deref()
            .map(|raw| decode_open_value(raw, OPEN_RECOVERY_MAX_BYTES))
            .transpose()?;
        checked_hot_guard(&workspace, &head, &lease, &guard, now)?;
        current.validate_for_guard(&guard, &base)?;
        validate_open_record(&open, guard.workspace_id)?;
        let open_matches = open.workspace_id == authority.open.workspace_id
            && open.owner_id == authority.open.owner_id
            && open.generation == authority.open.generation
            && open.expires_at_ns == authority.open.expires_at_ns
            && open.state == V3OpenState::Ready
            && !open.recovery_required;
        if current != result.binding
            || control.schema_version != WORKSPACE_SCHEMA_VERSION
            || workspace.active_lease != Some(lease.lease_id)
            || workspace.state != WorkspaceState::Active
            || !open_matches
            || open.expires_at_ns <= now
            || head.next_sequence != 1
            || head.owned_slice_count != 0
            || head.owned_bytes != 0
            || lease.base_revision != carrier
            || values[12].is_some()
            || values[13].is_some()
            || (name.is_some() && values.last().is_some_and(Option::is_some))
            || control.workspaces.get(&guard.workspace_id) != Some(&workspace)
            || control.layers.get(&head.layer_id) != Some(&head)
            || control.layers.get(&base.layer_id) != Some(&base)
            || control.leases.get(&lease.lease_id) != Some(&lease)
            || control.snapshots.contains_key(&result.snapshot_id)
            || name.as_ref().is_some_and(|name| {
                control
                    .snapshots
                    .values()
                    .any(|row| row.name.as_ref() == Some(name))
            })
            || recovery
                .as_ref()
                .is_some_and(|row| row.workspace_id != guard.workspace_id || row.incomplete)
            || workspace_has_incomplete_seal(&control, guard.workspace_id)
            || control.leases.values().any(|row| {
                row.workspace_id == guard.workspace_id
                    && row.writable
                    && row.lease_id != lease.lease_id
                    && (row.state == LeaseState::Active
                        || row.holder_generation >= lease.holder_generation
                        || row.created_at_ns >= lease.created_at_ns)
            })
        {
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[10])?;
        layer_inventory_generation(&values[11])?;
        let mut checks = basis.checks.clone();
        let (initial, immutable) = self
            .retained_initial_packed_bootstrap_checks(guard.workspace_id)
            .await?;
        if initial != authority.initial_binding {
            return Err(WorkspaceError::Fenced);
        }
        initial_merge(&mut checks, immutable)?;
        let (_, carrier_checks) = self.retained_packed_carrier_checks(&carrier).await?;
        initial_merge(&mut checks, carrier_checks)?;
        let deadline = lease.expires_at_ns.min(open.expires_at_ns);
        lease.state = LeaseState::Released;
        lease.updated_at_ns = now;
        open.expires_at_ns = now;
        let snapshot = SnapshotRecord {
            snapshot_id: result.snapshot_id,
            name,
            revision: carrier,
            owner_id,
            created_at_ns: now,
        };
        let receipt = PublishedInitialReceipt {
            packed_journal_id: record.journal_id,
            snapshot: snapshot.clone(),
            initial_binding: initial.encode()?,
            binding: result.binding.encode()?,
            native_sealed_source_revision: result.native_sealed_source_revision.clone(),
            released_lease: lease.clone(),
            closed_open: open.clone(),
        };
        control.leases.insert(lease.lease_id, lease.clone());
        let current = control
            .workspaces
            .get_mut(&guard.workspace_id)
            .ok_or(WorkspaceError::Fenced)?;
        current.active_lease = None;
        current.updated_at_ns = now;
        control
            .snapshots
            .insert(snapshot.snapshot_id, snapshot.clone());
        let mut writes = vec![
            put(open_v3_key(guard.workspace_id), &open)?,
            KvWrite::Put {
                key: initial_published_key(guard.workspace_id),
                value: encode_initial_receipt(&receipt)?,
            },
        ];
        self.stage_topology_diff(&basis, &control, &mut checks, &mut writes)?;
        let _writer_owner = self.prepare_administrative_packed_writer(
            guard.workspace_id, crate::workspace_overlay::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::Retire, &mut checks, &mut writes,
        ).await?;
        let _holds = self
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await?;
        // Only the caller that completed actual reader/transport shutdown can
        // enter this private finish. The durable snapshot and owners share CAS.
        self.initial_finish_exact_cas(&checks, &writes, deadline, &authority.budget)
            .await
    }

    /// Observe a genuine completed bootstrap. No new owner or mutation replay.
    pub async fn inspect_initial_packed_snapshot(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<PackedSnapshotResult>, WorkspaceError> {
        use crate::workspace_overlay::stores::kv_store::packed_journal::{
            PackedJournalPhase, PackedJournalRecord,
        };
        let budget = self
            .packed_reader_pin_budget
            .get()
            .ok_or(WorkspaceError::Fenced)?;
        let _owner = budget
            .admit(&[(V3BudgetPool::Metadata, INITIAL_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let key = initial_published_key(workspace_id);
        let (routing, _) = self
            .backend
            .get_many_consistent_with_time_bounded(std::slice::from_ref(&key), source_limits(1))
            .await?;
        if routing.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let Some(raw) = routing[0].as_deref() else {
            return Ok(None);
        };
        let receipt = decode_initial_receipt(raw)?;
        let binding = PackedLowerBindingRecord::decode(&receipt.binding)?;
        let initial = PackedLowerBindingRecord::decode(&receipt.initial_binding)?;
        let origin = InitialOperationOrigin::parse(&receipt.closed_open.owner_id)?
            .ok_or(WorkspaceError::Fenced)?;
        let keys = [
            key,
            hot_snapshot_key(receipt.snapshot.snapshot_id),
            hot_lease_key(
                receipt.released_lease.workspace_id,
                receipt.released_lease.lease_id,
            ),
            format!("packed/v3/journal/{}", receipt.packed_journal_id).into_bytes(),
        ];
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || now <= 0 || values[0] != routing[0] {
            return Err(WorkspaceError::Fenced);
        }
        let snapshot: SnapshotRecord = decode_open_value(
            values[1].as_deref().ok_or(WorkspaceError::Fenced)?,
            SOURCE_MAX_BYTES,
        )?;
        let lease: SnapshotLease = decode_open_value(
            values[2].as_deref().ok_or(WorkspaceError::Fenced)?,
            SOURCE_MAX_BYTES,
        )?;
        let record =
            PackedJournalRecord::decode(values[3].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        let (native_id, planned, source_version, carrier) =
            record.native_completion_identities()?;
        let source = record.final_source_guard().ok_or(WorkspaceError::Fenced)?;
        let (source_hash, _) = record
            .final_source_hash_facts()
            .ok_or(WorkspaceError::Fenced)?;
        if record.phase != PackedJournalPhase::Committed
            || record.commit_target.as_ref() != Some(&binding)
            || record.expected_binding != initial
            || binding.workspace_id != workspace_id
            || initial.workspace_id != workspace_id
            || native_id != origin.native_journal_id
            || planned != origin.planned_head
            || snapshot != receipt.snapshot
            || snapshot.snapshot_id != origin.snapshot_id
            || snapshot.revision != carrier
            || binding.base_revision != carrier
            || lease != receipt.released_lease
            || lease.state != LeaseState::Released
            || !lease.writable
            || lease.workspace_id != workspace_id
            || lease.base_revision != carrier
            || lease.lease_id != source.lease_id
            || lease.holder_generation != source.holder_generation
            || receipt.closed_open.workspace_id != workspace_id
            || receipt.closed_open.state != V3OpenState::Ready
            || receipt.closed_open.recovery_required
            || receipt.closed_open.expires_at_ns != lease.updated_at_ns
            || receipt.closed_open.expires_at_ns > now
            || receipt.native_sealed_source_revision.layer_id != record.guard.expected_head_layer_id
            || receipt.native_sealed_source_revision.sealed_version != source_version
            || receipt.native_sealed_source_revision.root_hash != source_hash
        {
            return Err(WorkspaceError::Fenced);
        }
        let (retained_initial, initial_checks) = self
            .retained_initial_packed_bootstrap_checks(workspace_id)
            .await?;
        if initial != retained_initial {
            return Err(WorkspaceError::Fenced);
        }
        let claim_raw = self
            .read_initial_bootstrap_claim(workspace_id)
            .await?
            .ok_or(WorkspaceError::Fenced)?;
        let provenance = InitialAdminClaim::decode(&claim_raw)?;
        if record.guard != provenance.source_guard.to_head_guard()
            || provenance.original_open.owner_id != receipt.closed_open.owner_id
        {
            return Err(WorkspaceError::Fenced);
        }
        let (_, carrier_checks) = self.retained_packed_carrier_checks(&carrier).await?;
        let mut checks = initial_exact(&keys, &values);
        initial_merge(&mut checks, initial_checks)?;
        initial_merge(&mut checks, carrier_checks)?;
        let _writer_owner = self
            .authenticate_idle_packed_writer(workspace_id, &mut checks)
            .await?;
        if !self
            .backend
            .authenticate_checks_before_bounded(
                &checks,
                checked_expiry(now, 30_000_000_000)?,
                source_limits(checks.len()),
            )
            .await?
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(Some(PackedSnapshotResult {
            snapshot_id: snapshot.snapshot_id,
            packed_carrier_revision: carrier,
            native_sealed_source_revision: receipt.native_sealed_source_revision,
            binding,
        }))
    }

    async fn restore_initial_before_seed(
        self: &Arc<Self>,
        workspace_id: WorkspaceId,
        request: &PackedHeadlessSnapshotRequest,
        layout: ChunkLayout,
        budget: Arc<V3MountBudget>,
    ) -> Result<Arc<PackedInitialBootstrapAuthority<B>>, WorkspaceError> {
        let (initial, immutable) = self
            .retained_initial_packed_bootstrap_checks(workspace_id)
            .await?;
        let keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(workspace_id),
            hot_layer_key(initial.head_layer_id),
            hot_layer_key(initial.base_revision.layer_id),
            open_v3_key(workspace_id),
            open_v3_recovery_key(workspace_id),
            initial_seed_key(request.native_journal_id),
            hot_layer_key(request.new_head_layer_id),
            packed_current_key(workspace_id),
            packed_claim_key(workspace_id),
            packed_history_key(workspace_id, 1),
            initial_published_key(workspace_id),
            hot_snapshot_key(request.snapshot_id),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
        ];
        let (basis, values) = self
            .initial_topology_view(
                &keys,
                workspace_id,
                &[request.lease_id],
                &[request.native_journal_id],
                true,
            )
            .await?;
        let now = basis.now_ns;
        if values.len() != keys.len()
            || now <= 0
            || values[5..8].iter().any(Option::is_some)
            || values[11..13].iter().any(Option::is_some)
        {
            return Err(WorkspaceError::Fenced);
        }
        let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
        let mut control = basis.state.clone();
        let workspace: WorkspaceRecord = decode_open_value(required(1)?, SOURCE_MAX_BYTES)?;
        let head: LayerRecord = decode_open_value(required(2)?, SOURCE_MAX_BYTES)?;
        let base: LayerRecord = decode_open_value(required(3)?, SOURCE_MAX_BYTES)?;
        let mut open: V3OpenRecord = decode_open_value(required(4)?, OPEN_RECORD_MAX_BYTES)?;
        let origin =
            InitialOperationOrigin::parse(&open.owner_id)?.ok_or(WorkspaceError::Fenced)?;
        let claim_raw = self
            .read_initial_bootstrap_claim(workspace_id)
            .await?
            .ok_or(WorkspaceError::Fenced)?;
        let mut provenance = InitialAdminClaim::decode(&claim_raw)?;
        if open != provenance.source_open {
            return Err(WorkspaceError::Fenced);
        }
        origin.matches_request(request, layout)?;
        validate_open_record(&open, workspace_id)?;
        let current = decode_packed_pair(workspace_id, &values[8], &values[9], &values[10])?
            .ok_or(WorkspaceError::Fenced)?;
        if current != initial
            || workspace.head_layer_id != initial.head_layer_id
            || workspace.head_epoch != initial.head_epoch
            || workspace.workspace_id != workspace_id
            || workspace.state != WorkspaceState::Active
            || head.layer_id != initial.head_layer_id
            || head.state != LayerState::Writable
            || head.next_sequence != 2
            || head.owned_bytes != 0
            || head.owned_slice_count != 0
            || open.state != V3OpenState::Ready
            || open.recovery_required
            || control.schema_version != WORKSPACE_SCHEMA_VERSION
            || control.workspaces.get(&workspace_id) != Some(&workspace)
            || control.layers.get(&head.layer_id) != Some(&head)
            || control.layers.get(&base.layer_id) != Some(&base)
            || workspace_has_incomplete_seal(&control, workspace_id)
            || control.journals.contains_key(&request.native_journal_id)
        {
            return Err(WorkspaceError::Fenced);
        }
        next_packed_root_generation(&values[13])?;
        layer_inventory_generation(&values[14])?;
        let previous = control
            .leases
            .values()
            .filter(|row| row.workspace_id == workspace_id && row.writable)
            .max_by_key(|row| row.holder_generation)
            .cloned()
            .ok_or(WorkspaceError::Fenced)?;
        if !matches!(previous.state, LeaseState::Active | LeaseState::Expired)
            || workspace
                .active_lease
                .is_some_and(|id| id != previous.lease_id)
            || (previous.state == LeaseState::Active
                && previous.expires_at_ns > now
                && workspace.active_lease != Some(previous.lease_id))
            || previous.base_revision != initial.base_revision
            || provenance.source_guard.lease_id != previous.lease_id
            || provenance.source_guard.holder_generation != previous.holder_generation
            || control.leases.values().any(|row| {
                row.workspace_id == workspace_id
                    && row.writable
                    && row.lease_id != previous.lease_id
                    && (row.state == LeaseState::Active
                        || row.holder_generation >= previous.holder_generation
                        || row.created_at_ns >= previous.created_at_ns)
            })
        {
            return Err(WorkspaceError::Fenced);
        }
        let lease_key = hot_lease_key(workspace_id, previous.lease_id);
        let (lease_values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                std::slice::from_ref(&lease_key),
                source_limits(1),
            )
            .await?;
        if lease_values.len() != 1
            || lease_values[0].as_deref() != Some(encode(&previous)?.as_slice())
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut checks = basis.checks.clone();
        initial_merge(&mut checks, immutable)?;
        checks.push(KvCheck {
            key: lease_key,
            expected: lease_values[0].clone(),
        });
        let mut lease = previous.clone();
        let mut writes = Vec::new();
        let mut not_before = None;
        if previous.state != LeaseState::Active || previous.expires_at_ns <= now {
            if request.lease_id == previous.lease_id
                || control.leases.contains_key(&request.lease_id)
            {
                return Err(WorkspaceError::Fenced);
            }
            let successor_key = hot_lease_key(workspace_id, request.lease_id);
            let (actual, _) = self
                .backend
                .get_many_consistent_with_time_bounded(
                    std::slice::from_ref(&successor_key),
                    source_limits(1),
                )
                .await?;
            if actual.len() != 1 || actual[0].is_some() {
                return Err(WorkspaceError::Fenced);
            }
            checks.push(KvCheck {
                key: successor_key,
                expected: None,
            });
            let mut expired = previous.clone();
            expired.state = LeaseState::Expired;
            expired.updated_at_ns = now;
            lease = SnapshotLease {
                lease_id: request.lease_id,
                workspace_id,
                base_revision: initial.base_revision.clone(),
                holder_generation: previous
                    .holder_generation
                    .checked_add(1)
                    .ok_or(WorkspaceError::Fenced)?,
                writable: true,
                state: LeaseState::Active,
                expires_at_ns: checked_expiry(now, request.lease_ttl_ns)?,
                created_at_ns: now,
                updated_at_ns: now,
            };
            control.leases.insert(expired.lease_id, expired.clone());
            control.leases.insert(lease.lease_id, lease.clone());
            let current = control
                .workspaces
                .get_mut(&workspace_id)
                .ok_or(WorkspaceError::Fenced)?;
            current.active_lease = Some(lease.lease_id);
            current.updated_at_ns = now;
            not_before = Some(previous.expires_at_ns);
        }
        if open.expires_at_ns <= now || lease.lease_id != previous.lease_id {
            if open.expires_at_ns <= now {
                open.generation = open
                    .generation
                    .checked_add(1)
                    .ok_or(WorkspaceError::Fenced)?;
            }
            open.expires_at_ns = checked_expiry(now, request.lease_ttl_ns)?;
            writes.push(put(open_v3_key(workspace_id), &open)?);
        }
        let guard = HeadGuard {
            workspace_id,
            expected_head_layer_id: initial.head_layer_id,
            expected_head_epoch: initial.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        };
        current.validate_for_guard(&guard, &base)?;
        let deadline = lease.expires_at_ns.min(open.expires_at_ns);
        if provenance.source_guard.to_head_guard() != guard || provenance.source_open != open {
            provenance.source_guard = (&guard).into();
            provenance.source_open = open.clone();
            let (check, write) = self
                .prepare_initial_bootstrap_claim(&initial, Some(&claim_raw), provenance.encode()?)
                .await?;
            initial_merge(&mut checks, [check])?;
            writes.push(write);
        }
        self.stage_topology_diff(&basis, &control, &mut checks, &mut writes)?;
        let _writer_owner = self.prepare_administrative_packed_writer(
            workspace_id, crate::workspace_overlay::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::Update, &mut checks, &mut writes,
        ).await?;
        let _holds = self
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await?;
        let packet = self
            .prepare_topology_envelope(checks, writes, Some(deadline))
            .await?;
        if !self
            .backend
            .compare_and_swap_in_time_window(
                &packet.checks,
                &packet.writes,
                not_before,
                Some(deadline),
            )
            .await?
        {
            return Err(WorkspaceError::Busy);
        }
        self.issue_initial_authority(guard, InitialAuthorityMode::Original, budget)
            .await
    }
}

impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// The only first open writer installs provenance in the actual PBI in the
    /// same CAS. An ordinary open with a prefix cannot manufacture this fact.
    async fn claim_initial_open(
        self: &Arc<Self>,
        guard: HeadGuard,
        origin: &InitialOperationOrigin,
        ttl_ns: u64,
        budget: Arc<V3MountBudget>,
    ) -> Result<Arc<PackedInitialBootstrapAuthority<B>>, WorkspaceError> {
        let (binding, immutable) = self
            .retained_initial_packed_bootstrap_checks(guard.workspace_id)
            .await?;
        self.initial_packed_bootstrap_checks(&guard, &binding)
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                initial_composer_diagnostic("claim-open-bootstrap-checks", _error);
            })?;
        let keys = [
            CONTROL_KEY.to_vec(),
            hot_workspace_key(guard.workspace_id),
            hot_layer_key(guard.expected_head_layer_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            open_v3_key(guard.workspace_id),
            open_v3_recovery_key(guard.workspace_id),
            initial_seed_key(origin.native_journal_id),
            hot_layer_key(origin.planned_head),
            hot_snapshot_key(origin.snapshot_id),
            initial_published_key(guard.workspace_id),
        ];
        let (basis, values) = self
            .initial_topology_view(
                &keys,
                guard.workspace_id,
                &[guard.lease_id],
                &[origin.native_journal_id],
                true,
            )
            .await?;
        let now = basis.now_ns;
        if values.len() != keys.len() || now <= 0 || values[4..].iter().any(Option::is_some) {
            #[cfg(test)]
            eprintln!(
                "[packed-v3-initial-composer-diag] stage=claim-open-predecessor count_matches={} clock_positive={} occupied_successor={}",
                values.len() == keys.len(),
                now > 0,
                values
                    .get(4..)
                    .is_some_and(|rows| rows.iter().any(Option::is_some))
            );
            return Err(WorkspaceError::Fenced);
        }
        let control = basis.state.clone();
        let workspace: WorkspaceRecord = decode_open_value(
            values[1].as_deref().ok_or(WorkspaceError::Fenced)?,
            SOURCE_MAX_BYTES,
        )?;
        let head: LayerRecord = decode_open_value(
            values[2].as_deref().ok_or(WorkspaceError::Fenced)?,
            SOURCE_MAX_BYTES,
        )?;
        let lease: SnapshotLease = decode_open_value(
            values[3].as_deref().ok_or(WorkspaceError::Fenced)?,
            SOURCE_MAX_BYTES,
        )?;
        checked_hot_guard(&workspace, &head, &lease, &guard, now)?;
        if control.workspaces.get(&workspace.workspace_id) != Some(&workspace)
            || workspace.active_lease != Some(lease.lease_id)
            || control.layers.get(&head.layer_id) != Some(&head)
            || control.leases.get(&lease.lease_id) != Some(&lease)
            || control.snapshots.contains_key(&origin.snapshot_id)
            || control.journals.contains_key(&origin.native_journal_id)
            || workspace_has_incomplete_seal(&control, guard.workspace_id)
            || head.next_sequence != 2
            || binding.head_layer_id != guard.expected_head_layer_id
            || binding.head_epoch != guard.expected_head_epoch
        {
            #[cfg(test)]
            eprintln!(
                "[packed-v3-initial-composer-diag] stage=claim-open-source workspace_matches={} head_matches={} lease_matches={} snapshot_absent={} journal_absent={} no_incomplete_seal={} sequence_initial={} binding_head_matches={} binding_epoch_matches={}",
                control.workspaces.get(&workspace.workspace_id) == Some(&workspace),
                control.layers.get(&head.layer_id) == Some(&head),
                control.leases.get(&lease.lease_id) == Some(&lease),
                !control.snapshots.contains_key(&origin.snapshot_id),
                !control.journals.contains_key(&origin.native_journal_id),
                !workspace_has_incomplete_seal(&control, guard.workspace_id),
                head.next_sequence == 2,
                binding.head_layer_id == guard.expected_head_layer_id,
                binding.head_epoch == guard.expected_head_epoch
            );
            return Err(WorkspaceError::Fenced);
        }
        let open = V3OpenRecord {
            workspace_id: guard.workspace_id,
            owner_id: origin.owner_id(),
            generation: 1,
            expires_at_ns: checked_expiry(now, ttl_ns)?,
            state: V3OpenState::Ready,
            recovery_required: false,
        };
        let claim = InitialAdminClaim {
            original_guard: (&guard).into(),
            original_open: open.clone(),
            source_guard: (&guard).into(),
            source_open: open.clone(),
        };
        let (pbi_check, pbi_write) = self
            .prepare_initial_bootstrap_claim(&binding, None, claim.encode()?)
            .await?;
        let mut checks = basis.checks.clone();
        initial_merge(&mut checks, immutable)?;
        initial_merge(&mut checks, [pbi_check])?;
        let mut writes = vec![put(open_v3_key(guard.workspace_id), &open)?, pbi_write];
        let _writer_owner = self.prepare_administrative_packed_writer(
            guard.workspace_id, crate::workspace_overlay::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::ClaimInitial, &mut checks, &mut writes,
        ).await?;
        self.clean_exact_cas(
            &checks,
            &writes,
            open.expires_at_ns.min(lease.expires_at_ns),
        )
        .await?;
        self.issue_initial_authority(guard, InitialAuthorityMode::Original, budget)
            .await
    }
}

#[cfg(target_os = "linux")]
impl<B: WorkspaceKvBackend> KvWorkspaceStore<B> {
    /// A root-only native volume is created without an old native snapshot;
    /// actual first install, runtime drain and native publication follow.
    pub async fn bootstrap_initial_packed_snapshot<O, S>(
        self: &Arc<Self>,
        volume: CreateVolumeRoot,
        client: ObjectClient<O>,
        upper: Arc<S>,
        layout: ChunkLayout,
        request: PackedHeadlessSnapshotRequest,
    ) -> Result<PackedSnapshotResult, PackedHeadlessSnapshotFailure>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        self.require_admin_access()?;
        let store = self.clone();
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            WorkspaceError::CorruptMetadata(
                "initial packed snapshot requires a running Tokio runtime".into(),
            )
        })?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        handle.spawn(async move {
            let result = store
                .bootstrap_initial_packed_snapshot_owned(volume, client, upper, layout, request)
                .await;
            let _ = sender.send(result);
        });
        receiver.await.map_err(|_| {
            PackedHeadlessSnapshotFailure::from(WorkspaceError::CorruptMetadata(
                "owned initial packed snapshot driver stopped".into(),
            ))
        })?
    }

    async fn bootstrap_initial_packed_snapshot_owned<O, S>(
        self: &Arc<Self>,
        volume: CreateVolumeRoot,
        client: ObjectClient<O>,
        upper: Arc<S>,
        layout: ChunkLayout,
        request: PackedHeadlessSnapshotRequest,
    ) -> Result<PackedSnapshotResult, PackedHeadlessSnapshotFailure>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        if volume.workspace_id.as_uuid().is_nil()
            || request.lease_id.as_uuid().is_nil()
            || request.lease_ttl_ns == 0
            || request.lease_ttl_ns > 15 * 60 * 1_000_000_000
            || request.max_rows == 0
            || request.scratch_disk_bytes < 16 << 10
            || layout.chunk_size == 0
            || request
                .temporary_owner
                .as_ref()
                .is_some_and(|owner| owner.path() != request.temporary)
            || request.cancel.is_cancelled()
        {
            return Err(WorkspaceError::Fenced.into());
        }
        let origin = InitialOperationOrigin::from_request(&request, layout)?;
        let budget = self.resolve_packed_reader_pin_budget(V3MountBudget::defaults());
        if budget.state().closed {
            return Err(WorkspaceError::Fenced.into());
        }
        let _driver_owner = budget
            .admit(&[(V3BudgetPool::Metadata, INITIAL_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let _temporary_owner = request.temporary_owner.clone();
        let observer = budget
            .read_observer(client.read_observer())
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let client = client.with_read_observer(
            observer,
            crate::cadapter::read_observer::Engine::PackedV3,
            crate::cadapter::read_observer::Phase::Startup,
            crate::cadapter::read_observer::Origin::Demand,
        );
        self.ensure_initial_volume_root(&volume)
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                initial_composer_diagnostic("ensure-volume-root", _error);
            })?;
        if let Some(result) = self
            .inspect_initial_packed_snapshot(volume.workspace_id)
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                initial_composer_diagnostic("inspect-published", _error);
            })?
        {
            let keys = [initial_published_key(volume.workspace_id)];
            let (values, _) = self
                .backend
                .get_many_consistent_with_time_bounded(&keys, source_limits(1))
                .await?;
            if values.len() != 1 {
                return Err(WorkspaceError::Fenced.into());
            }
            let receipt =
                decode_initial_receipt(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
            let actual_origin = InitialOperationOrigin::parse(&receipt.closed_open.owner_id)?
                .ok_or(WorkspaceError::Fenced)?;
            actual_origin.matches_request(&request, layout)?;
            if result.snapshot_id != request.snapshot_id
                || receipt.snapshot.name != request.snapshot_name
                || receipt.snapshot.owner_id != request.owner_id
            {
                return Err(WorkspaceError::Fenced.into());
            }
            budget.close();
            return Ok(result);
        }
        let keys = [
            open_v3_key(volume.workspace_id),
            initial_seed_key(request.native_journal_id),
            hot_snapshot_key(request.snapshot_id),
        ];
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, source_limits(keys.len()))
            .await?;
        if values.len() != keys.len() || values[2].is_some() {
            return Err(WorkspaceError::Fenced.into());
        }
        if let Some(raw) = values[0].as_deref() {
            let open: V3OpenRecord = decode_open_value(raw, OPEN_RECORD_MAX_BYTES)?;
            let actual_origin =
                InitialOperationOrigin::parse(&open.owner_id)?.ok_or(WorkspaceError::Fenced)?;
            actual_origin.matches_request(&request, layout)?;
            if values[1].is_some() {
                return self
                    .recover_initial_native_snapshot(
                        volume.workspace_id,
                        client,
                        upper,
                        layout,
                        request,
                        budget,
                    )
                    .await;
            }
            let authority = self
                .restore_initial_before_seed(volume.workspace_id, &request, layout, budget.clone())
                .await?;
            return self
                .run_initial_native_snapshot(authority, None, client, upper, layout, request)
                .await;
        }
        if values[1].is_some() {
            return Err(WorkspaceError::Fenced.into());
        }
        let guard = self
            .initial_lease_before_install(&volume, &request)
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                initial_composer_diagnostic("initial-lease", _error);
            })?;
        let binding = self
            .bootstrap_initial_packed_lower(
                guard.clone(),
                client.clone(),
                request.temporary.clone(),
                request.producer.clone(),
                budget.clone(),
                request.cancel.clone(),
            )
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                initial_composer_diagnostic("bootstrap-lower", _error);
            })?;
        let guard = HeadGuard {
            expected_head_epoch: binding.head_epoch,
            ..guard
        };
        let authority = self
            .claim_initial_open(guard, &origin, request.lease_ttl_ns, budget)
            .await
            .inspect_err(|_error| {
                #[cfg(test)]
                initial_composer_diagnostic("claim-open", _error);
            })?;
        authority.origin.matches_request(&request, layout)?;
        self.run_initial_native_snapshot(authority, None, client, upper, layout, request)
            .await
            .inspect_err(|_failure| {
                #[cfg(test)]
                initial_composer_diagnostic("run-native", _failure.error());
            })
    }

    async fn run_initial_native_snapshot<O, S>(
        self: &Arc<Self>,
        authority: Arc<PackedInitialBootstrapAuthority<B>>,
        recovery: Option<Arc<crate::workspace_overlay::stores::kv_store::packed_native_freeze::PackedNativeRecoveryReadFence<B>>>,
        client: ObjectClient<O>,
        upper: Arc<S>,
        layout: ChunkLayout,
        request: PackedHeadlessSnapshotRequest,
    ) -> Result<PackedSnapshotResult, PackedHeadlessSnapshotFailure>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        use crate::chunk::read_plan::WorkspaceReadPlanProvider;
        use crate::workspace_overlay::meta_layer::PinnedCatalogPackedBindingAuthority;
        use crate::workspace_overlay::packed_reader_lifecycle::{
            KvPackedReaderSession, PackedReaderLeaseOptions, PackedReaderSession,
        };
        authority.origin.matches_request(&request, layout)?;
        let budget = authority.budget.clone();
        let binding = authority.initial_binding.clone();
        let guard = authority.guard.clone();
        let runtime = Arc::new(InitialRuntimeOwner {
            authority: authority.clone(),
            store: self.clone(),
            budget: budget.clone(),
            upper: upper.clone(),
            temporary_owner: request.temporary_owner.clone(),
            cleanup_handle: tokio::runtime::Handle::current(),
            metadata: std::sync::Mutex::new(None),
            reader: std::sync::Mutex::new(None),
            native: std::sync::Mutex::new(None),
            finished: std::sync::atomic::AtomicBool::new(false),
        });
        let snapshot = AuthenticatedV3Snapshot::open(&client, &binding.binding.manifest)
            .await
            .map_err(|error| {
                initial_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?;
        let lower = Arc::new(
            PackedV3ReadonlyMeta::from_v3_budget(
                client.clone(),
                snapshot,
                layout.chunk_size,
                0,
                budget.clone(),
            )
            .map_err(|error| {
                initial_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?,
        );
        let initial_metadata = WorkspaceMetaLayer::with_chunk_size(
            self.clone(),
            ViewContext {
                workspace_id: guard.workspace_id,
                head_layer_id: guard.expected_head_layer_id,
                head_epoch: guard.expected_head_epoch,
                lease_id: guard.lease_id,
                holder_generation: guard.holder_generation,
            },
            layout.chunk_size,
        );
        let metadata = if let Some(recovery) = &recovery {
            let reader: Arc<dyn PackedReaderSession> = KvPackedReaderSession::open_native_recovery(
                recovery.clone(),
                budget.clone(),
                PackedReaderLeaseOptions::default(),
            )
            .await
            .map_err(|error| initial_failure(error, &runtime))?;
            *runtime.reader.lock().unwrap() = Some(reader.clone());
            let pinned = Arc::new(PinnedCatalogPackedBindingAuthority {
                store: self.clone(),
                reader,
            });
            Arc::new(
                initial_metadata
                    .with_packed_v3_lower(
                        binding.binding.clone(),
                        lower,
                        pinned,
                        upper.clone(),
                        layout,
                    )
                    .map_err(|error| {
                        initial_failure(
                            WorkspaceError::CorruptMetadata(error.to_string()),
                            &runtime,
                        )
                    })?,
            )
        } else {
            Arc::new(
                initial_metadata
                    .with_packed_v3_lower_from_store_owned(lower, upper.clone(), layout, |reader| {
                        *runtime.reader.lock().unwrap() = Some(reader);
                    })
                    .await
                    .map_err(|error| {
                        initial_failure(
                            WorkspaceError::CorruptMetadata(error.to_string()),
                            &runtime,
                        )
                    })?,
            )
        };
        *runtime.metadata.lock().unwrap() = Some(metadata.clone());
        let vfs = if recovery.is_some() {
            let provider: Arc<dyn WorkspaceReadPlanProvider> = metadata.clone();
            VFS::from_readonly_components_with_provider(
                VFSConfig::new(layout),
                upper,
                metadata.clone(),
                provider,
            )
        } else {
            metadata.initialize().await.map_err(|error| {
                initial_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?;
            VFS::from_workspace_components(VFSConfig::new(layout), upper, metadata.clone())
        }
        .map_err(|error| {
            initial_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
        })?;
        let local = vfs.quiesce_packed_vfs().await.map_err(|error| {
            initial_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
        })?;
        let native = if let Some(recovery) = &recovery {
            recovery.native_quiesce().clone()
        } else {
            let keys = [
                hot_layer_key(binding.head_layer_id),
                hot_layer_key(binding.base_revision.layer_id),
            ];
            let (values, _) = self
                .backend
                .get_many_consistent_with_time_bounded(&keys, source_limits(2))
                .await
                .map_err(|error| initial_failure(error, &runtime))?;
            if values.len() != 2 {
                return Err(initial_failure(WorkspaceError::Fenced, &runtime));
            }
            let layers = [
                decode_open_value(
                    values[0]
                        .as_deref()
                        .ok_or_else(|| initial_failure(WorkspaceError::Fenced, &runtime))?,
                    SOURCE_MAX_BYTES,
                )?,
                decode_open_value(
                    values[1]
                        .as_deref()
                        .ok_or_else(|| initial_failure(WorkspaceError::Fenced, &runtime))?,
                    SOURCE_MAX_BYTES,
                )?,
            ];
            Arc::new(
                self.clone()
                    .begin_initial_packed_native_quiesce(
                        authority.clone(),
                        layers,
                        request.native_journal_id,
                        request.new_head_layer_id,
                    )
                    .await
                    .inspect_err(|_error| {
                        #[cfg(test)]
                        initial_composer_diagnostic("begin-native-quiesce", _error);
                    })
                    .map_err(|error| initial_failure(error, &runtime))?,
            )
        };
        *runtime.native.lock().unwrap() = Some(native.clone());
        let artifact = FrozenNativeArtifact::capture(
            native,
            local,
            request.temporary.clone(),
            NativeCaptureLimits {
                max_inodes: request.max_rows,
                max_names: request.max_rows,
                max_spans: request.max_rows,
                max_logical_bytes: request.max_logical_bytes,
                max_data_bytes: request.max_data_bytes,
                max_payload_disk_bytes: request.scratch_disk_bytes,
                max_sqlite_disk_bytes: request.scratch_disk_bytes,
                max_producer_spool_disk_bytes: request.scratch_disk_bytes,
                sqlite_cache_bytes: 64 << 10,
                max_sql_vm_steps: 1_000_000,
            },
            request.cancel.clone(),
        )
        .await
        .map_err(|error| {
            initial_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
        })?;
        let options = NativePublicationBuildOptions {
            producer: request.producer.clone(),
            temporary: request.temporary.clone(),
            graph_scratch: request.temporary.clone(),
            graph_limits: request.graph_limits,
            native_hash_limits: NativeDeltaHashLimits {
                max_native_delta_rows: request.max_rows,
                max_canonical_bytes: request.scratch_disk_bytes,
            },
            chunk_size: layout.chunk_size,
            metadata_cache_bytes: 0,
            max_catalog_rows: request.max_rows,
            cancel: request.cancel.clone(),
        };
        let producer_client = client;
        let ready = if let Some(recovery) = recovery {
            if recovery.basis().is_some() {
                self.resume_frozen_native_publication(artifact, recovery, producer_client, options)
                    .await
            } else {
                self.prepare_frozen_native_publication(artifact, producer_client, options)
                    .await
            }
        } else {
            self.prepare_frozen_native_publication(artifact, producer_client, options)
                .await
        }
        .map_err(|failure| {
            PackedHeadlessSnapshotFailure::held((failure, runtime.clone()), |owner| {
                match &owner.0 {
                    NativePublicationPreparationFailure::BeforeHashed(error)
                    | NativePublicationPreparationFailure::HashedAdmission { error, .. }
                    | NativePublicationPreparationFailure::Hashed { error, .. } => error,
                    NativePublicationPreparationFailure::Promotion { failure, .. } => {
                        &failure.error
                    }
                }
            })
        })?;
        let outcome = ready.commit().await.map_err(|failure| {
            PackedHeadlessSnapshotFailure::held((failure, runtime.clone()), |owner| &owner.0.error)
        })?;
        let result = PackedSnapshotResult {
            snapshot_id: request.snapshot_id,
            packed_carrier_revision: outcome.binding.base_revision.clone(),
            native_sealed_source_revision: outcome.sealed_source,
            binding: outcome.binding,
        };
        let cleanup = async {
            metadata
                .shutdown_packed_runtime_for_clean_release()
                .await
                .map_err(|error| WorkspaceError::CorruptMetadata(error.to_string()))?;
            self.finish_initial_publication(
                &authority,
                &result,
                &outcome.record,
                request.snapshot_name,
                request.owner_id,
            )
            .await
        }
        .await;
        if let Err(error) = cleanup {
            let mut failure = initial_failure(error, &runtime);
            failure.committed = Some(Box::new(result));
            return Err(failure);
        }
        budget.close();
        runtime
            .finished
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(result)
    }

    async fn recover_initial_native_snapshot<O, S>(
        self: &Arc<Self>,
        workspace_id: WorkspaceId,
        client: ObjectClient<O>,
        upper: Arc<S>,
        layout: ChunkLayout,
        request: PackedHeadlessSnapshotRequest,
        budget: Arc<V3MountBudget>,
    ) -> Result<PackedSnapshotResult, PackedHeadlessSnapshotFailure>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        use crate::workspace_overlay::stores::kv_store::packed_journal::{
            PackedJournalPhase, PackedJournalRecord,
        };
        let claim_raw = self
            .read_initial_bootstrap_claim(workspace_id)
            .await?
            .ok_or(WorkspaceError::Fenced)?;
        let provenance = InitialAdminClaim::decode(&claim_raw)?;
        let origin = InitialOperationOrigin::parse(&provenance.original_open.owner_id)?
            .ok_or(WorkspaceError::Fenced)?;
        origin.matches_request(&request, layout)?;
        let (initial, _) = self
            .retained_initial_packed_bootstrap_checks(workspace_id)
            .await?;
        let route = self
            .read_clean_native_journal_route(
                workspace_id,
                origin.native_journal_id,
                origin.planned_head,
                &budget,
            )
            .await?;
        let record = if let Some(id) = route {
            let key = format!("packed/v3/journal/{id}").into_bytes();
            let (values, _) = self
                .backend
                .get_many_consistent_with_time_bounded(&[key], source_limits(1))
                .await?;
            if values.len() != 1 {
                return Err(WorkspaceError::Fenced.into());
            }
            let record =
                PackedJournalRecord::decode(values[0].as_deref().ok_or(WorkspaceError::Fenced)?)?;
            if record.journal_id != id
                || record.expected_binding != initial
                || record.guard != provenance.source_guard.to_head_guard()
                || record.phase == PackedJournalPhase::Aborted
            {
                return Err(WorkspaceError::Fenced.into());
            }
            if record.phase == PackedJournalPhase::Committed {
                return self
                    .finish_committed_initial_snapshot(
                        record, client, upper, layout, request, budget,
                    )
                    .await;
            }
            Some(record)
        } else {
            None
        };
        let keys = [
            open_v3_key(workspace_id),
            hot_lease_key(workspace_id, provenance.source_guard.lease_id),
            format!(
                "packed/v3/native-recovery-claim/{}",
                origin.native_journal_id
            )
            .into_bytes(),
            CONTROL_KEY.to_vec(),
        ];
        let (catalog_basis, values) = self
            .initial_topology_view(
                &keys,
                workspace_id,
                &[provenance.source_guard.lease_id],
                &[origin.native_journal_id],
                true,
            )
            .await?;
        let now = catalog_basis.now_ns;
        if now <= 0 {
            return Err(WorkspaceError::Fenced.into());
        }
        let open: V3OpenRecord = decode_open_value(
            values[0].as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        let original_lease: SnapshotLease = decode_open_value(
            values[1].as_deref().ok_or(WorkspaceError::Fenced)?,
            SOURCE_MAX_BYTES,
        )?;
        let control = &catalog_basis.state;
        if open.owner_id != origin.owner_id()
            || original_lease.lease_id != provenance.source_guard.lease_id
            || original_lease.holder_generation != provenance.source_guard.holder_generation
            || original_lease.workspace_id != workspace_id
            || original_lease.base_revision != initial.base_revision
        {
            return Err(WorkspaceError::Fenced.into());
        }
        let original_live = values[2].is_none()
            && original_lease.state == LeaseState::Active
            && original_lease.expires_at_ns > now
            && open == provenance.source_open
            && open.state == V3OpenState::Ready
            && !open.recovery_required
            && open.expires_at_ns > now;
        let basis = if let Some(record) = &record {
            Some(
                self.inspect_native_packed_recovery_basis(record, &budget)
                    .await?,
            )
        } else {
            None
        };
        if original_live && let Some(basis) = basis {
            let recovery = self
                .clone()
                .reissue_native_source_read(basis, budget.clone())
                .await?;
            let recovery = self
                .quarantine_missing_native_attempt_and_reissue(
                    recovery,
                    &client,
                    request.graph_limits.max_objects,
                    &request.cancel,
                )
                .await?;
            let authority = recovery
                .native_quiesce()
                .initial_publication_origin()
                .ok_or(WorkspaceError::Fenced)?;
            if authority.mode != InitialAuthorityMode::Original {
                return Err(WorkspaceError::Fenced.into());
            }
            return self
                .run_initial_native_snapshot(
                    authority,
                    Some(recovery),
                    client,
                    upper,
                    layout,
                    request,
                )
                .await;
        }
        let open = self
            .open_workspace_v3(
                workspace_id,
                origin.owner_id(),
                std::time::Duration::from_nanos(request.lease_ttl_ns),
            )
            .await?;
        if open.state != V3OpenState::Recovering || !open.recovery_required {
            return Err(WorkspaceError::Fenced.into());
        }
        let latest = control
            .leases
            .values()
            .filter(|row| row.workspace_id == workspace_id && row.writable)
            .max_by_key(|row| row.holder_generation)
            .ok_or(WorkspaceError::Fenced)?;
        let recovery = if let Some(basis) = basis {
            let lease_id = if values[2].is_some()
                && latest.state == LeaseState::Active
                && latest.expires_at_ns > now
            {
                latest.lease_id
            } else {
                request.lease_id
            };
            let owner = self
                .claim_native_packed_recovery(
                    basis,
                    NativePackedRecoveryClaimRequest {
                        new_lease_id: lease_id,
                        owner_id: origin.owner_id(),
                        ttl_ns: request.lease_ttl_ns,
                    },
                    budget.clone(),
                )
                .await?;
            let fresh = self
                .inspect_native_packed_recovery_basis(
                    record.as_ref().ok_or(WorkspaceError::Fenced)?,
                    &budget,
                )
                .await?;
            self.clone()
                .reissue_native_source_read_claimed(fresh, owner, budget.clone())
                .await?
        } else {
            let retain = latest.state == LeaseState::Active && latest.expires_at_ns > now;
            let native = self
                .recover_packed_native_prepare(
                    NativePrepareRecoveryRequest {
                        journal_id: request.native_journal_id,
                        owner_id: origin.owner_id(),
                        new_lease_id: (!retain).then_some(request.lease_id),
                        ttl_ns: request.lease_ttl_ns,
                    },
                    budget.clone(),
                )
                .await?;
            self.reissue_native_seed_read(native, budget.clone())
                .await?
        };
        let recovery = if recovery.basis().is_some() {
            self.quarantine_missing_native_attempt_and_reissue(
                recovery,
                &client,
                request.graph_limits.max_objects,
                &request.cancel,
            )
            .await?
        } else {
            recovery
        };
        let authority = recovery
            .native_quiesce()
            .initial_publication_origin()
            .ok_or(WorkspaceError::Fenced)?;
        if authority.mode != InitialAuthorityMode::Recovery {
            return Err(WorkspaceError::Fenced.into());
        }
        self.run_initial_native_snapshot(authority, Some(recovery), client, upper, layout, request)
            .await
    }

    /// A genuine committed PPJ admits completion and runtime teardown only.
    /// It cannot be converted into a new native source or publication replay.
    async fn finish_committed_initial_snapshot<O, S>(
        self: &Arc<Self>,
        mut record: crate::workspace_overlay::stores::kv_store::packed_journal::PackedJournalRecord,
        client: ObjectClient<O>,
        upper: Arc<S>,
        layout: ChunkLayout,
        request: PackedHeadlessSnapshotRequest,
        budget: Arc<V3MountBudget>,
    ) -> Result<PackedSnapshotResult, PackedHeadlessSnapshotFailure>
    where
        O: ObjectBackend + Clone + 'static,
        S: BlockStore + Send + Sync + 'static,
    {
        use crate::chunk::read_plan::WorkspaceReadPlanProvider;
        use crate::workspace_overlay::stores::kv_store::packed_journal::PackedJournalPhase;
        let owner = budget
            .admit(&[(V3BudgetPool::Metadata, INITIAL_BYTES)])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let workspace_id = record.guard.workspace_id;
        let (initial, initial_checks) = self
            .retained_initial_packed_bootstrap_checks(workspace_id)
            .await?;
        let claim_raw = self
            .read_initial_bootstrap_claim(workspace_id)
            .await?
            .ok_or(WorkspaceError::Fenced)?;
        let provenance = InitialAdminClaim::decode(&claim_raw)?;
        let origin = InitialOperationOrigin::parse(&provenance.original_open.owner_id)?
            .ok_or(WorkspaceError::Fenced)?;
        origin.matches_request(&request, layout)?;
        let mut ownership_attempted = false;
        let (authority, result) = loop {
            let (native_id, planned, source_version, carrier) =
                record.native_completion_identities()?;
            let (source_hash, _) = record
                .final_source_hash_facts()
                .ok_or(WorkspaceError::Fenced)?;
            let final_source = record
                .final_source_guard()
                .ok_or(WorkspaceError::Fenced)?
                .clone();
            let target = record
                .commit_target
                .as_ref()
                .ok_or(WorkspaceError::Fenced)?
                .clone();
            if record.phase != PackedJournalPhase::Committed
                || record.expected_binding != initial
                || record.guard != provenance.source_guard.to_head_guard()
                || native_id != origin.native_journal_id
                || planned != origin.planned_head
                || target.base_revision != carrier
            {
                return Err(WorkspaceError::Fenced.into());
            }
            let guard = HeadGuard {
                expected_head_layer_id: target.head_layer_id,
                expected_head_epoch: target.head_epoch,
                ..final_source.clone()
            };
            let keys = vec![
                CONTROL_KEY.to_vec(),
                format!("packed/v3/journal/{}", record.journal_id).into_bytes(),
                hot_workspace_key(workspace_id),
                hot_layer_key(target.head_layer_id),
                hot_layer_key(carrier.layer_id),
                hot_lease_key(workspace_id, guard.lease_id),
                open_v3_key(workspace_id),
                open_v3_recovery_key(workspace_id),
                packed_current_key(workspace_id),
                packed_claim_key(workspace_id),
                packed_history_key(workspace_id, target.binding.binding_version),
                PACKED_ROOT_GENERATION_KEY.to_vec(),
                LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                format!("packed/v3/journal-active/{}", record.journal_id).into_bytes(),
                format!("packed/v3/native-recovery-claim/{native_id}").into_bytes(),
                initial_published_key(workspace_id),
                hot_snapshot_key(request.snapshot_id),
            ];
            let (basis, values) = self
                .initial_topology_view(
                    &keys,
                    workspace_id,
                    &[guard.lease_id, request.lease_id],
                    &[native_id],
                    true,
                )
                .await?;
            let now = basis.now_ns;
            if now <= 0
                || values[1].as_deref() != Some(record.encode()?.as_slice())
                || values[13..].iter().any(Option::is_some)
            {
                return Err(WorkspaceError::Fenced.into());
            }
            let required = |index: usize| values[index].as_deref().ok_or(WorkspaceError::Fenced);
            let mut control = basis.state.clone();
            let workspace: WorkspaceRecord = decode_open_value(required(2)?, SOURCE_MAX_BYTES)?;
            let head: LayerRecord = decode_open_value(required(3)?, SOURCE_MAX_BYTES)?;
            let base: LayerRecord = decode_open_value(required(4)?, SOURCE_MAX_BYTES)?;
            let lease: SnapshotLease = decode_open_value(required(5)?, SOURCE_MAX_BYTES)?;
            let mut open: V3OpenRecord = decode_open_value(required(6)?, OPEN_RECORD_MAX_BYTES)?;
            let recovery: Option<V3RecoveryRecord> = values[7]
                .as_deref()
                .map(|raw| decode_open_value(raw, OPEN_RECOVERY_MAX_BYTES))
                .transpose()?;
            let current = decode_packed_pair(workspace_id, &values[8], &values[9], &values[10])?
                .ok_or(WorkspaceError::Fenced)?;
            current.validate_for_guard(&guard, &base)?;
            validate_open_record(&open, workspace_id)?;
            if control.schema_version != WORKSPACE_SCHEMA_VERSION
                || current != target
                || workspace.workspace_id != workspace_id
                || workspace.head_layer_id != target.head_layer_id
                || workspace.head_epoch != target.head_epoch
                || workspace.state != WorkspaceState::Active
                || head.layer_id != target.head_layer_id
                || head.state != LayerState::Writable
                || head.next_sequence != 1
                || head.owned_slice_count != 0
                || head.owned_bytes != 0
                || lease.lease_id != guard.lease_id
                || lease.holder_generation != guard.holder_generation
                || lease.workspace_id != workspace_id
                || !lease.writable
                || lease.base_revision != carrier
                || !matches!(lease.state, LeaseState::Active | LeaseState::Expired)
                || open.owner_id != origin.owner_id()
                || open.state != V3OpenState::Ready
                || open.recovery_required
                || recovery
                    .as_ref()
                    .is_some_and(|row| row.workspace_id != workspace_id || row.incomplete)
                || control.workspaces.get(&workspace_id) != Some(&workspace)
                || control.layers.get(&head.layer_id) != Some(&head)
                || control.layers.get(&base.layer_id) != Some(&base)
                || control.leases.get(&lease.lease_id) != Some(&lease)
                || workspace
                    .active_lease
                    .is_some_and(|id| id != lease.lease_id)
                || (lease.state == LeaseState::Active
                    && lease.expires_at_ns > now
                    && workspace.active_lease != Some(lease.lease_id))
                || workspace_has_incomplete_seal(&control, workspace_id)
                || control.leases.values().any(|row| {
                    row.workspace_id == workspace_id
                        && row.writable
                        && row.lease_id != lease.lease_id
                        && (row.state == LeaseState::Active
                            || row.holder_generation >= lease.holder_generation
                            || row.created_at_ns >= lease.created_at_ns)
                })
            {
                return Err(WorkspaceError::Fenced.into());
            }
            next_packed_root_generation(&values[11])?;
            layer_inventory_generation(&values[12])?;
            let mut checks = basis.checks.clone();
            initial_merge(&mut checks, initial_checks.clone())?;
            let (_, carrier_checks) = self.retained_packed_carrier_checks(&carrier).await?;
            initial_merge(&mut checks, carrier_checks)?;
            if lease.state != LeaseState::Active
                || lease.expires_at_ns <= now
                || open.expires_at_ns <= now
            {
                if ownership_attempted {
                    return Err(WorkspaceError::Fenced.into());
                }
                ownership_attempted = true;
                if open.expires_at_ns <= now {
                    open.generation = open
                        .generation
                        .checked_add(1)
                        .ok_or(WorkspaceError::Fenced)?;
                }
                open.expires_at_ns = checked_expiry(now, request.lease_ttl_ns)?;
                let mut writes = vec![put(open_v3_key(workspace_id), &open)?];
                let mut next_record = record.clone();
                let not_before = if lease.state != LeaseState::Active || lease.expires_at_ns <= now
                {
                    if request.lease_id == lease.lease_id
                        || control.leases.contains_key(&request.lease_id)
                    {
                        return Err(WorkspaceError::Fenced.into());
                    }
                    let mut expired = lease.clone();
                    expired.state = LeaseState::Expired;
                    expired.updated_at_ns = now;
                    let successor = SnapshotLease {
                        lease_id: request.lease_id,
                        workspace_id,
                        base_revision: carrier.clone(),
                        holder_generation: lease
                            .holder_generation
                            .checked_add(1)
                            .ok_or(WorkspaceError::Fenced)?,
                        writable: true,
                        state: LeaseState::Active,
                        expires_at_ns: open.expires_at_ns,
                        created_at_ns: now,
                        updated_at_ns: now,
                    };
                    control.leases.insert(expired.lease_id, expired.clone());
                    control.leases.insert(successor.lease_id, successor.clone());
                    let workspace = control
                        .workspaces
                        .get_mut(&workspace_id)
                        .ok_or(WorkspaceError::Fenced)?;
                    workspace.active_lease = Some(successor.lease_id);
                    workspace.updated_at_ns = now;
                    next_record = record.with_final_completion_guard(HeadGuard {
                        lease_id: successor.lease_id,
                        holder_generation: successor.holder_generation,
                        ..final_source.clone()
                    })?;
                    self.stage_topology_diff(&basis, &control, &mut checks, &mut writes)?;
                    writes.push(KvWrite::Put {
                        key: format!("packed/v3/journal/{}", record.journal_id).into_bytes(),
                        value: next_record.encode()?,
                    });
                    Some(lease.expires_at_ns)
                } else {
                    None
                };
                let _writer_owner = self.prepare_administrative_packed_writer(
                    workspace_id, crate::workspace_overlay::stores::kv_store::packed_writer_authority::AdministrativeWriterTransition::Update, &mut checks, &mut writes,
                ).await?;
                let _holds = self
                    .prepare_native_owner_cas(&mut checks, &mut writes)
                    .await?;
                let deadline = if lease.state == LeaseState::Active && lease.expires_at_ns > now {
                    lease.expires_at_ns.min(open.expires_at_ns)
                } else {
                    open.expires_at_ns
                };
                // Unknown replies do not resend this ownership mutation; a
                // fresh retry re-reads its exact actual final-source successor.
                let packet = self
                    .prepare_topology_envelope(checks, writes, Some(deadline))
                    .await?;
                if !self
                    .backend
                    .compare_and_swap_in_time_window(
                        &packet.checks,
                        &packet.writes,
                        not_before,
                        packet.deadline,
                    )
                    .await?
                {
                    return Err(WorkspaceError::Busy.into());
                }
                record = next_record;
                continue;
            }
            let _writer_owner = self
                .authenticate_administrative_packed_writer(workspace_id, &mut checks)
                .await?;
            checked_hot_guard(&workspace, &head, &lease, &guard, now)?;
            if !self
                .backend
                .compare_and_swap_before(&checks, &[], open.expires_at_ns.min(lease.expires_at_ns))
                .await?
            {
                return Err(WorkspaceError::Busy.into());
            }
            let authority = Arc::new(PackedInitialBootstrapAuthority {
                store: self.clone(),
                initial_binding: initial.clone(),
                immutable: initial_checks.clone(),
                open,
                guard: final_source,
                origin: origin.clone(),
                budget: budget.clone(),
                mode: InitialAuthorityMode::Committed,
                _owner: owner,
            });
            let result = PackedSnapshotResult {
                snapshot_id: request.snapshot_id,
                packed_carrier_revision: carrier,
                native_sealed_source_revision: BaseRevision {
                    layer_id: record.guard.expected_head_layer_id,
                    sealed_version: source_version,
                    root_hash: source_hash,
                },
                binding: target,
            };
            break (authority, result);
        };
        let runtime = Arc::new(InitialRuntimeOwner {
            authority: authority.clone(),
            store: self.clone(),
            budget: budget.clone(),
            upper: upper.clone(),
            temporary_owner: request.temporary_owner.clone(),
            cleanup_handle: tokio::runtime::Handle::current(),
            metadata: std::sync::Mutex::new(None),
            reader: std::sync::Mutex::new(None),
            native: std::sync::Mutex::new(None),
            finished: std::sync::atomic::AtomicBool::new(false),
        });
        let snapshot = AuthenticatedV3Snapshot::open(&client, &result.binding.binding.manifest)
            .await
            .map_err(|error| {
                initial_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?;
        let lower = Arc::new(
            PackedV3ReadonlyMeta::from_v3_budget(
                client,
                snapshot,
                layout.chunk_size,
                0,
                budget.clone(),
            )
            .map_err(|error| {
                initial_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?,
        );
        let guard = HeadGuard {
            expected_head_layer_id: result.binding.head_layer_id,
            expected_head_epoch: result.binding.head_epoch,
            ..authority.guard.clone()
        };
        let metadata = Arc::new(
            WorkspaceMetaLayer::with_chunk_size(
                self.clone(),
                ViewContext {
                    workspace_id: guard.workspace_id,
                    head_layer_id: guard.expected_head_layer_id,
                    head_epoch: guard.expected_head_epoch,
                    lease_id: guard.lease_id,
                    holder_generation: guard.holder_generation,
                },
                layout.chunk_size,
            )
            .with_packed_v3_lower_from_store_owned(lower, upper.clone(), layout, |reader| {
                *runtime.reader.lock().unwrap() = Some(reader);
            })
            .await
            .map_err(|error| {
                initial_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
            })?,
        );
        *runtime.metadata.lock().unwrap() = Some(metadata.clone());
        let provider: Arc<dyn WorkspaceReadPlanProvider> = metadata.clone();
        let vfs = VFS::from_readonly_components_with_provider(
            VFSConfig::new(layout),
            upper,
            metadata.clone(),
            provider,
        )
        .map_err(|error| {
            initial_failure(WorkspaceError::CorruptMetadata(error.to_string()), &runtime)
        })?;
        let cleanup = async {
            let _local = vfs
                .quiesce_packed_vfs()
                .await
                .map_err(|error| WorkspaceError::CorruptMetadata(error.to_string()))?;
            metadata
                .shutdown_packed_runtime_for_clean_release()
                .await
                .map_err(|error| WorkspaceError::CorruptMetadata(error.to_string()))?;
            self.finish_initial_publication(
                &authority,
                &result,
                &record,
                request.snapshot_name,
                request.owner_id,
            )
            .await
        }
        .await;
        if let Err(error) = cleanup {
            let mut failure = initial_failure(error, &runtime);
            failure.committed = Some(Box::new(result));
            return Err(failure);
        }
        budget.close();
        runtime
            .finished
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(result)
    }
}
