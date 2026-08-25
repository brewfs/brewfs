//! Backend-neutral workspace catalog stored in Redis or TiKV.
//!
//! Lifecycle transitions retain a versioned topology document for atomicity.
//! Hot workspace heads, layer sequences, leases, and allocators are mirrored in
//! independently CAS-able records, so agent writes and heartbeats do not contend
//! on that topology document.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::Mutex;

use super::kv_backend::{KvCheck, KvEntry, KvWrite, WorkspaceKvBackend};
use crate::workspace_overlay::catalog::*;
use crate::workspace_overlay::digest::{CanonicalLayerDelta, delta_digest, root_hash};
use crate::workspace_overlay::error::{ConflictDetail, WorkspaceError};
use crate::workspace_overlay::ids::{JournalId, LayerId, LeaseId, SnapshotId, WorkspaceId};
use crate::workspace_overlay::model::*;
use crate::workspace_overlay::resolver::validate_layer_chain;

const CONTROL_KEY: &[u8] = b"control";
const HOT_WORKSPACE_PREFIX: &[u8] = b"hot/workspace/";
const HOT_LAYER_PREFIX: &[u8] = b"hot/layer/";
const HOT_LEASE_PREFIX: &[u8] = b"hot/lease/";
const HOT_ALLOCATOR_PREFIX: &[u8] = b"hot/allocator/";
const ENVELOPE_MAGIC: &[u8; 8] = b"BWSKV001";
const CAS_MAX_RETRIES: usize = 64;
const VOLUME_FORMAT: &str = "workspace-v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ControlState {
    schema_version: u32,
    header: Option<VolumeHeader>,
    workspaces: BTreeMap<WorkspaceId, WorkspaceRecord>,
    layers: BTreeMap<LayerId, LayerRecord>,
    snapshots: BTreeMap<SnapshotId, SnapshotRecord>,
    leases: BTreeMap<LeaseId, SnapshotLease>,
    journals: BTreeMap<JournalId, SealJournal>,
    allocators: BTreeMap<String, i64>,
}

impl Default for ControlState {
    fn default() -> Self {
        Self {
            schema_version: WORKSPACE_SCHEMA_VERSION,
            header: None,
            workspaces: BTreeMap::new(),
            layers: BTreeMap::new(),
            snapshots: BTreeMap::new(),
            leases: BTreeMap::new(),
            journals: BTreeMap::new(),
            allocators: BTreeMap::new(),
        }
    }
}

pub struct KvWorkspaceStore<B> {
    backend: Arc<B>,
    topology_gate: Mutex<()>,
}

impl<B> KvWorkspaceStore<B>
where
    B: WorkspaceKvBackend,
{
    pub fn new(backend: B) -> Self {
        Self {
            backend: Arc::new(backend),
            topology_gate: Mutex::new(()),
        }
    }

    pub fn from_arc(backend: Arc<B>) -> Self {
        Self {
            backend,
            topology_gate: Mutex::new(()),
        }
    }

    async fn load_control_raw(
        &self,
    ) -> Result<
        (
            Option<Vec<u8>>,
            ControlState,
            BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        ),
        WorkspaceError,
    > {
        for _ in 0..CAS_MAX_RETRIES {
            let raw = self.backend.get(CONTROL_KEY).await?;
            let mut state: ControlState =
                raw.as_deref().map(decode).transpose()?.unwrap_or_default();
            if state.schema_version != WORKSPACE_SCHEMA_VERSION {
                return Err(WorkspaceError::UnsupportedSchemaVersion(
                    state.schema_version,
                ));
            }
            let mut hot = BTreeMap::new();
            self.hydrate_hot_state(&mut state, &mut hot).await?;
            if self.backend.get(CONTROL_KEY).await? == raw {
                return Ok((raw, state, hot));
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn load_control(&self) -> Result<ControlState, WorkspaceError> {
        Ok(self.load_control_raw().await?.1)
    }

    async fn update_control<R, F>(&self, mut operation: F) -> Result<R, WorkspaceError>
    where
        F: FnMut(&mut ControlState, &mut Vec<KvWrite>) -> Result<R, WorkspaceError>,
    {
        let _guard = self.topology_gate.lock().await;
        for _ in 0..CAS_MAX_RETRIES {
            let (raw, mut state, hot) = self.load_control_raw().await?;
            let before = state.clone();
            let mut writes = Vec::new();
            let result = operation(&mut state, &mut writes)?;
            append_hot_diff(&before, &state, &mut writes)?;
            writes.push(KvWrite::Put {
                key: CONTROL_KEY.to_vec(),
                value: encode(&state)?,
            });
            let mut checks = vec![KvCheck {
                key: CONTROL_KEY.to_vec(),
                expected: raw,
            }];
            checks.extend(hot.iter().map(|(key, expected)| KvCheck {
                key: key.clone(),
                expected: expected.clone(),
            }));
            for write in &writes {
                let key = match write {
                    KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
                };
                if is_hot_key(key) && !checks.iter().any(|check| check.key == *key) {
                    checks.push(KvCheck {
                        key: key.clone(),
                        expected: hot.get(key).cloned().unwrap_or(None),
                    });
                }
            }
            if self.backend.compare_and_swap(&checks, &writes).await? {
                return Ok(result);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn hydrate_hot_state(
        &self,
        state: &mut ControlState,
        raw: &mut BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> Result<(), WorkspaceError> {
        for entry in self.backend.scan_prefix(HOT_WORKSPACE_PREFIX).await? {
            let row: WorkspaceRecord = decode(&entry.value)?;
            state.workspaces.insert(row.workspace_id, row);
            raw.insert(entry.key, Some(entry.value));
        }
        for entry in self.backend.scan_prefix(HOT_LAYER_PREFIX).await? {
            let row: LayerRecord = decode(&entry.value)?;
            state.layers.insert(row.layer_id, row);
            raw.insert(entry.key, Some(entry.value));
        }
        for entry in self.backend.scan_prefix(HOT_LEASE_PREFIX).await? {
            let row: SnapshotLease = decode(&entry.value)?;
            state.leases.insert(row.lease_id, row);
            raw.insert(entry.key, Some(entry.value));
        }
        for entry in self.backend.scan_prefix(HOT_ALLOCATOR_PREFIX).await? {
            let name = allocator_name_from_key(&entry.key)?;
            let value: i64 = decode(&entry.value)?;
            state.allocators.insert(name, value);
            raw.insert(entry.key, Some(entry.value));
        }
        Ok(())
    }

    async fn load_hot<T: DeserializeOwned>(
        &self,
        key: Vec<u8>,
    ) -> Result<(Option<Vec<u8>>, Option<T>), WorkspaceError> {
        let raw = self.backend.get(&key).await?;
        let value = raw.as_deref().map(decode).transpose()?;
        Ok((raw, value))
    }

    async fn hot_mutation<R, F>(
        &self,
        guard: &HeadGuard,
        mut operation: F,
    ) -> Result<R, WorkspaceError>
    where
        F: FnMut(&mut LayerRecord, &mut Vec<KvWrite>) -> Result<R, WorkspaceError>,
    {
        let workspace_key = hot_workspace_key(guard.workspace_id);
        let layer_key = hot_layer_key(guard.expected_head_layer_id);
        let lease_key = hot_lease_key(guard.lease_id);
        for _ in 0..CAS_MAX_RETRIES {
            let keys = [workspace_key.clone(), layer_key.clone(), lease_key.clone()];
            let (values, now) = self.backend.get_many_with_time(&keys).await?;
            let mut values = values.into_iter();
            let workspace_raw = values.next().flatten();
            let layer_raw = values.next().flatten();
            let lease_raw = values.next().flatten();
            let workspace = workspace_raw.as_deref().map(decode).transpose()?;
            let layer = layer_raw.as_deref().map(decode).transpose()?;
            let lease = lease_raw.as_deref().map(decode).transpose()?;
            let workspace =
                workspace.ok_or(WorkspaceError::WorkspaceNotFound(guard.workspace_id))?;
            let mut layer = layer.ok_or(WorkspaceError::Fenced)?;
            let lease = lease.ok_or(WorkspaceError::Fenced)?;
            checked_hot_guard(&workspace, &layer, &lease, guard, now)?;
            let mut writes = Vec::new();
            let result = operation(&mut layer, &mut writes)?;
            writes.push(put(layer_key.clone(), &layer)?);
            let checks = [
                KvCheck {
                    key: workspace_key.clone(),
                    expected: workspace_raw,
                },
                KvCheck {
                    key: layer_key.clone(),
                    expected: layer_raw,
                },
                KvCheck {
                    key: lease_key.clone(),
                    expected: lease_raw,
                },
            ];
            if self.backend.compare_and_swap(&checks, &writes).await? {
                return Ok(result);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn now_ns(&self) -> Result<i64, WorkspaceError> {
        self.backend.server_time_ns().await
    }

    async fn scan<T: DeserializeOwned>(&self, prefix: Vec<u8>) -> Result<Vec<T>, WorkspaceError> {
        self.backend
            .scan_prefix(&prefix)
            .await?
            .into_iter()
            .map(|entry| decode(&entry.value))
            .collect()
    }

    async fn scan_entries(&self, prefix: Vec<u8>) -> Result<Vec<KvEntry>, WorkspaceError> {
        self.backend.scan_prefix(&prefix).await
    }

    async fn layer_delta_unchecked(
        &self,
        layer_id: LayerId,
    ) -> Result<CanonicalLayerDelta, WorkspaceError> {
        let mut delta = CanonicalLayerDelta {
            dentries: self.scan(dentry_layer_prefix(layer_id)).await?,
            inodes: self.scan(inode_layer_prefix(layer_id)).await?,
            xattrs: self.scan(xattr_layer_prefix(layer_id)).await?,
            acls: self.scan(acl_layer_prefix(layer_id)).await?,
            extents: self.scan(extent_layer_prefix(layer_id)).await?,
        };
        sort_delta(&mut delta);
        Ok(delta)
    }
}

#[async_trait]
impl<B> WorkspaceStore for KvWorkspaceStore<B>
where
    B: WorkspaceKvBackend,
{
    fn name(&self) -> &'static str {
        self.backend.name()
    }

    fn capabilities(&self) -> WorkspaceStoreCapabilities {
        WorkspaceStoreCapabilities {
            atomic_head_switch: true,
            durable_lease: true,
            transactional_namespace_mutation: true,
            transactional_rename: true,
            watch_head_change: false,
        }
    }

    async fn initialize_workspace_schema(&self) -> Result<(), WorkspaceError> {
        self.update_control(|_, _| Ok(())).await
    }

    async fn load_volume_header(&self) -> Result<Option<VolumeHeader>, WorkspaceError> {
        let state: ControlState = self
            .backend
            .get(CONTROL_KEY)
            .await?
            .as_deref()
            .map(decode)
            .transpose()?
            .unwrap_or_default();
        if state.schema_version != WORKSPACE_SCHEMA_VERSION {
            return Err(WorkspaceError::UnsupportedSchemaVersion(
                state.schema_version,
            ));
        }
        Ok(state.header)
    }

    async fn load_workspace(&self, id: WorkspaceId) -> Result<WorkspaceRecord, WorkspaceError> {
        self.load_hot(hot_workspace_key(id))
            .await?
            .1
            .ok_or(WorkspaceError::WorkspaceNotFound(id))
    }

    async fn load_layer(&self, id: LayerId) -> Result<LayerRecord, WorkspaceError> {
        self.load_hot(hot_layer_key(id))
            .await?
            .1
            .ok_or(WorkspaceError::LayerNotFound(id))
    }

    async fn load_layer_chain(&self, head: LayerId) -> Result<Vec<LayerRecord>, WorkspaceError> {
        let mut chain = Vec::new();
        let mut current = Some(head);
        while let Some(layer_id) = current {
            if chain.len() > LAYER_CHAIN_HARD_LIMIT as usize {
                return Err(WorkspaceError::LayerDepthLimit {
                    depth: chain.len() as u32,
                    hard_limit: LAYER_CHAIN_HARD_LIMIT,
                });
            }
            let layer = self.load_layer(layer_id).await?;
            current = layer.parent_layer_id;
            chain.push(layer);
        }
        validate_layer_chain(head, &chain)?;
        Ok(chain)
    }

    async fn allocate_id(&self, name: &str) -> Result<i64, WorkspaceError> {
        if !matches!(name, "inode" | "slice" | "sealed_version") {
            return Err(WorkspaceError::CorruptMetadata(format!(
                "unknown workspace allocator {name}"
            )));
        }
        let key = hot_allocator_key(name);
        for _ in 0..CAS_MAX_RETRIES {
            let (raw, value) = self.load_hot::<i64>(key.clone()).await?;
            let current = value.unwrap_or(1);
            let next = current
                .checked_add(1)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("allocator overflows".into()))?;
            let writes = [put(key.clone(), &next)?];
            let checks = [KvCheck {
                key: key.clone(),
                expected: raw,
            }];
            if self.backend.compare_and_swap(&checks, &writes).await? {
                return Ok(current);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn create_volume_root(
        &self,
        request: CreateVolumeRoot,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        if request.root_layer_id == request.writable_layer_id {
            return Err(WorkspaceError::CorruptMetadata(
                "root and writable layer IDs must differ".into(),
            ));
        }
        let now = self.now_ns().await?;
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
        let workspace = WorkspaceRecord {
            workspace_id: request.workspace_id,
            head_layer_id: request.writable_layer_id,
            head_epoch: 0,
            fork_base: Some(BaseRevision {
                layer_id: request.root_layer_id,
                sealed_version: 1,
                root_hash: root,
            }),
            owner_id: request.owner_id.clone(),
            state: WorkspaceState::Active,
            created_at_ns: now,
            updated_at_ns: now,
        };
        self.update_control(|state, writes| {
            if state.header.is_some() {
                return Err(WorkspaceError::InvalidStateTransition {
                    from: "initialized".into(),
                    to: "create-volume-root".into(),
                });
            }
            state.layers.insert(
                request.root_layer_id,
                LayerRecord {
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
                },
            );
            state.layers.insert(
                request.writable_layer_id,
                writable_layer(
                    request.writable_layer_id,
                    request.root_layer_id,
                    2,
                    request.workspace_id,
                    now,
                ),
            );
            state
                .workspaces
                .insert(request.workspace_id, workspace.clone());
            state.allocators.insert("inode".into(), 2);
            state.allocators.insert("slice".into(), 1);
            state.allocators.insert("sealed_version".into(), 2);
            state.header = Some(VolumeHeader {
                volume_format: VOLUME_FORMAT.into(),
                schema_version: WORKSPACE_SCHEMA_VERSION,
                volume_id: request.volume_id,
                created_at_ns: now,
            });
            writes.push(put(inode_key(&root_inode), &root_inode)?);
            Ok(workspace.clone())
        })
        .await
    }

    async fn create_workspace(
        &self,
        request: CreateWorkspace,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        let now = self.now_ns().await?;
        let base_key = hot_layer_key(request.base_revision.layer_id);
        let workspace_key = hot_workspace_key(request.workspace_id);
        let head_key = hot_layer_key(request.head_layer_id);
        for _ in 0..CAS_MAX_RETRIES {
            let (base_raw, base) = self.load_hot::<LayerRecord>(base_key.clone()).await?;
            let base = base.ok_or(WorkspaceError::LayerNotFound(
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
                head_epoch: 0,
                fork_base: Some(request.base_revision.clone()),
                owner_id: request.owner_id.clone(),
                state: WorkspaceState::Active,
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
            let checks = [
                KvCheck {
                    key: base_key.clone(),
                    expected: base_raw,
                },
                KvCheck {
                    key: workspace_key.clone(),
                    expected: None,
                },
                KvCheck {
                    key: head_key.clone(),
                    expected: None,
                },
            ];
            let writes = [
                put(workspace_key.clone(), &workspace)?,
                put(head_key.clone(), &head)?,
            ];
            if self.backend.compare_and_swap(&checks, &writes).await? {
                return Ok(workspace);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceError> {
        let mut rows = self
            .load_control()
            .await?
            .workspaces
            .into_values()
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| (row.created_at_ns, row.workspace_id));
        Ok(rows)
    }

    async fn create_snapshot(
        &self,
        request: CreateSnapshot,
    ) -> Result<SnapshotRecord, WorkspaceError> {
        let now = self.now_ns().await?;
        self.update_control(|state, _| {
            if revision_state(state, request.revision.layer_id)? != request.revision {
                return Err(conflict("snapshot revision changed"));
            }
            if state.snapshots.contains_key(&request.snapshot_id)
                || request.name.as_ref().is_some_and(|name| {
                    state
                        .snapshots
                        .values()
                        .any(|snapshot| snapshot.name.as_ref() == Some(name))
                })
            {
                return Err(conflict("snapshot ID or name already exists"));
            }
            let snapshot = SnapshotRecord {
                snapshot_id: request.snapshot_id,
                name: request.name.clone(),
                revision: request.revision.clone(),
                owner_id: request.owner_id.clone(),
                created_at_ns: now,
            };
            state
                .snapshots
                .insert(request.snapshot_id, snapshot.clone());
            Ok(snapshot)
        })
        .await
    }

    async fn load_snapshot(&self, id: SnapshotId) -> Result<SnapshotRecord, WorkspaceError> {
        self.load_control()
            .await?
            .snapshots
            .get(&id)
            .cloned()
            .ok_or_else(|| WorkspaceError::Backend(format!("snapshot not found: {id}")))
    }

    async fn list_snapshots(&self) -> Result<Vec<SnapshotRecord>, WorkspaceError> {
        let mut rows = self
            .load_control()
            .await?
            .snapshots
            .into_values()
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| (row.created_at_ns, row.snapshot_id));
        Ok(rows)
    }

    async fn delete_snapshot(&self, id: SnapshotId) -> Result<(), WorkspaceError> {
        self.update_control(|state, _| {
            state
                .snapshots
                .remove(&id)
                .map(|_| ())
                .ok_or_else(|| WorkspaceError::Backend(format!("snapshot not found: {id}")))
        })
        .await
    }

    async fn acquire_lease(&self, request: AcquireLease) -> Result<SnapshotLease, WorkspaceError> {
        if request.ttl_ns == 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "lease TTL must be positive".into(),
            ));
        }
        let now = self.now_ns().await?;
        let expires = checked_expiry(now, request.ttl_ns)?;
        self.update_control(|state, _| {
            let workspace = state
                .workspaces
                .get(&request.workspace_id)
                .ok_or(WorkspaceError::WorkspaceNotFound(request.workspace_id))?;
            if workspace.state != WorkspaceState::Active {
                return Err(WorkspaceError::Busy);
            }
            let head = state
                .layers
                .get(&workspace.head_layer_id)
                .ok_or(WorkspaceError::LayerNotFound(workspace.head_layer_id))?;
            if head.state != LayerState::Writable {
                return Err(WorkspaceError::Busy);
            }
            let parent = head.parent_layer_id.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("writable head has no parent".into())
            })?;
            let base_revision = revision_state(state, parent)?;
            for lease in state.leases.values_mut() {
                if lease.workspace_id == request.workspace_id
                    && lease.state == LeaseState::Active
                    && lease.expires_at_ns <= now
                {
                    lease.state = LeaseState::Expired;
                    lease.updated_at_ns = now;
                }
            }
            if state.leases.contains_key(&request.lease_id)
                || state.leases.values().any(|lease| {
                    lease.workspace_id == request.workspace_id
                        && lease.writable
                        && lease.state == LeaseState::Active
                })
            {
                return Err(WorkspaceError::Busy);
            }
            let lease = SnapshotLease {
                lease_id: request.lease_id,
                workspace_id: request.workspace_id,
                base_revision,
                holder_generation: request.holder_generation,
                writable: true,
                state: LeaseState::Active,
                expires_at_ns: expires,
                created_at_ns: now,
                updated_at_ns: now,
            };
            state.leases.insert(request.lease_id, lease.clone());
            Ok(lease)
        })
        .await
    }

    async fn renew_lease(&self, request: RenewLease) -> Result<SnapshotLease, WorkspaceError> {
        if request.ttl_ns == 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "lease TTL must be positive".into(),
            ));
        }
        let now = self.now_ns().await?;
        let expires = checked_expiry(now, request.ttl_ns)?;
        let key = hot_lease_key(request.lease_id);
        for _ in 0..CAS_MAX_RETRIES {
            let (raw, lease) = self.load_hot::<SnapshotLease>(key.clone()).await?;
            let mut lease = lease.ok_or(WorkspaceError::Fenced)?;
            if lease.holder_generation != request.holder_generation
                || lease.state != LeaseState::Active
                || lease.expires_at_ns <= now
            {
                return Err(WorkspaceError::Fenced);
            }
            lease.expires_at_ns = expires;
            lease.updated_at_ns = now;
            let writes = [put(key.clone(), &lease)?];
            let checks = [KvCheck {
                key: key.clone(),
                expected: raw,
            }];
            if self.backend.compare_and_swap(&checks, &writes).await? {
                return Ok(lease);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn release_lease(&self, request: ReleaseLease) -> Result<(), WorkspaceError> {
        let now = self.now_ns().await?;
        self.update_control(|state, _| {
            let lease = state
                .leases
                .get_mut(&request.lease_id)
                .ok_or(WorkspaceError::Fenced)?;
            if lease.holder_generation != request.holder_generation
                || lease.state != LeaseState::Active
            {
                return Err(WorkspaceError::Fenced);
            }
            lease.state = LeaseState::Released;
            lease.updated_at_ns = now;
            Ok(())
        })
        .await
    }

    async fn reap_expired_leases(&self) -> Result<u64, WorkspaceError> {
        let now = self.now_ns().await?;
        self.update_control(|state, _| {
            let mut count = 0_u64;
            for lease in state.leases.values_mut() {
                if lease.state == LeaseState::Active && lease.expires_at_ns <= now {
                    lease.state = LeaseState::Expired;
                    lease.updated_at_ns = now;
                    count += 1;
                }
            }
            Ok(count)
        })
        .await
    }

    async fn list_leases(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<SnapshotLease>, WorkspaceError> {
        let mut rows = self
            .load_control()
            .await?
            .leases
            .into_values()
            .filter(|lease| lease.workspace_id == workspace_id)
            .collect::<Vec<_>>();
        rows.sort_by_key(|row| (row.created_at_ns, row.lease_id));
        Ok(rows)
    }

    async fn get_dentry_deltas(
        &self,
        request: DentryQuery,
    ) -> Result<Vec<DentryDelta>, WorkspaceError> {
        if let Some(name) = request.name {
            let keys = request
                .layer_ids
                .iter()
                .map(|layer| dentry_identity_key(*layer, request.parent_ino, &name))
                .collect::<Vec<_>>();
            return self
                .backend
                .get_many(&keys)
                .await?
                .into_iter()
                .flatten()
                .map(|value| decode(&value))
                .collect();
        }
        let mut rows = Vec::new();
        for layer in request.layer_ids {
            let mut found: Vec<DentryDelta> = self
                .scan(dentry_parent_prefix(layer, request.parent_ino))
                .await?;
            found.sort_by(|left, right| left.name.cmp(&right.name));
            rows.extend(found);
        }
        Ok(rows)
    }

    async fn get_inode_deltas(
        &self,
        request: InodeQuery,
    ) -> Result<Vec<InodeDelta>, WorkspaceError> {
        let keys = request
            .layer_ids
            .iter()
            .map(|layer| inode_identity_key(*layer, request.ino))
            .collect::<Vec<_>>();
        self.backend
            .get_many(&keys)
            .await?
            .into_iter()
            .flatten()
            .map(|value| decode(&value))
            .collect()
    }

    async fn get_extent_deltas(
        &self,
        request: ExtentQuery,
    ) -> Result<Vec<DataExtentDelta>, WorkspaceError> {
        if request.range_start > request.range_end {
            return Err(WorkspaceError::InvalidReadPlan(
                "extent query starts after its end".into(),
            ));
        }
        if request.range_start == request.range_end {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for layer in request.layer_ids {
            let mut found: Vec<DataExtentDelta> = self
                .scan(extent_chunk_prefix(layer, request.ino, request.chunk_index))
                .await?;
            found.retain(|row| {
                row.logical_offset < request.range_end
                    && row
                        .logical_offset
                        .saturating_add(row.length)
                        .gt(&request.range_start)
            });
            found.sort_by_key(|row| std::cmp::Reverse(row.sequence));
            rows.extend(found);
        }
        Ok(rows)
    }

    async fn get_xattr_deltas(
        &self,
        request: XattrQuery,
    ) -> Result<Vec<XattrDelta>, WorkspaceError> {
        if let Some(name) = request.name {
            let keys = request
                .layer_ids
                .iter()
                .map(|layer| xattr_identity_key(*layer, request.ino, &name))
                .collect::<Vec<_>>();
            return self
                .backend
                .get_many(&keys)
                .await?
                .into_iter()
                .flatten()
                .map(|value| decode(&value))
                .collect();
        }
        let mut rows = Vec::new();
        for layer in request.layer_ids {
            let mut found: Vec<XattrDelta> =
                self.scan(xattr_inode_prefix(layer, request.ino)).await?;
            found.sort_by(|left, right| left.name.cmp(&right.name));
            rows.extend(found);
        }
        Ok(rows)
    }

    async fn get_acl_deltas(&self, request: AclQuery) -> Result<Vec<AclDelta>, WorkspaceError> {
        if request.acl_type.is_some() != request.acl_id.is_some() {
            return Err(WorkspaceError::CorruptMetadata(
                "ACL type and ID filters must be provided together".into(),
            ));
        }
        if let (Some(acl_type), Some(acl_id)) = (request.acl_type, request.acl_id) {
            let keys = request
                .layer_ids
                .iter()
                .map(|layer| acl_identity_key(*layer, request.ino, acl_type, acl_id))
                .collect::<Vec<_>>();
            return self
                .backend
                .get_many(&keys)
                .await?
                .into_iter()
                .flatten()
                .map(|value| decode(&value))
                .collect();
        }
        let mut rows = Vec::new();
        for layer in request.layer_ids {
            let mut found: Vec<AclDelta> = self.scan(acl_inode_prefix(layer, request.ino)).await?;
            found.sort_by_key(|row| (row.acl_type, row.acl_id));
            rows.extend(found);
        }
        Ok(rows)
    }

    async fn apply_namespace_mutation(
        &self,
        request: NamespaceMutation,
    ) -> Result<MutationResult, WorkspaceError> {
        if request.dentries.is_empty() && request.inodes.is_empty() {
            return Ok(MutationResult {
                first_sequence: None,
                last_sequence: None,
            });
        }
        for dentry in &request.dentries {
            if dentry.layer_id != request.guard.expected_head_layer_id {
                return Err(WorkspaceError::Fenced);
            }
            dentry.validate()?;
        }
        if request
            .inodes
            .iter()
            .any(|inode| inode.layer_id != request.guard.expected_head_layer_id)
        {
            return Err(WorkspaceError::Fenced);
        }
        let count = request
            .dentries
            .len()
            .checked_add(request.inodes.len())
            .ok_or_else(|| WorkspaceError::CorruptMetadata("mutation is too large".into()))?;
        self.hot_mutation(&request.guard, |layer, writes| {
            let range =
                allocate_layer_sequences(layer, count)?.expect("non-empty mutation has a range");
            let mut sequence = range.0;
            for template in &request.dentries {
                let mut row = template.clone();
                row.sequence = sequence;
                writes.push(put(dentry_key(&row), &row)?);
                sequence += 1;
            }
            for template in &request.inodes {
                let mut row = template.clone();
                row.sequence = sequence;
                writes.push(put(inode_key(&row), &row)?);
                sequence += 1;
            }
            Ok(MutationResult {
                first_sequence: Some(range.0),
                last_sequence: Some(range.1),
            })
        })
        .await
    }

    async fn apply_inode_mutation(
        &self,
        request: InodeMutation,
    ) -> Result<InodeDelta, WorkspaceError> {
        if request.inode.layer_id != request.guard.expected_head_layer_id {
            return Err(WorkspaceError::Fenced);
        }
        self.hot_mutation(&request.guard, |layer, writes| {
            let mut inode = request.inode.clone();
            inode.sequence = allocate_layer_sequences(layer, 1)?
                .expect("single mutation has a sequence")
                .0;
            writes.push(put(inode_key(&inode), &inode)?);
            Ok(inode)
        })
        .await
    }

    async fn append_data_extent(
        &self,
        request: AppendDataExtent,
    ) -> Result<DataExtentDelta, WorkspaceError> {
        validate_extent_request(
            &request.extent,
            request.guard.expected_head_layer_id,
            request.chunk_size,
        )?;
        self.hot_mutation(&request.guard, |layer, writes| {
            let mut extent = request.extent.clone();
            extent.sequence = allocate_layer_sequences(layer, 1)?
                .expect("single mutation has a sequence")
                .0;
            if matches!(extent.kind, ExtentKind::Data { .. }) {
                layer.owned_slice_count = layer
                    .owned_slice_count
                    .checked_add(1)
                    .ok_or_else(|| WorkspaceError::Backend("owned slice count overflows".into()))?;
                layer.owned_bytes = layer
                    .owned_bytes
                    .checked_add(extent.length)
                    .ok_or_else(|| WorkspaceError::Backend("owned byte count overflows".into()))?;
            }
            writes.push(put(extent_key(&extent), &extent)?);
            Ok(extent)
        })
        .await
    }

    async fn apply_data_mutation(
        &self,
        request: DataMutation,
    ) -> Result<DataMutationResult, WorkspaceError> {
        let head = request.guard.expected_head_layer_id;
        if request.inode.layer_id != head
            || request
                .extents
                .iter()
                .any(|extent| extent.layer_id != head || extent.ino != request.inode.ino)
        {
            return Err(WorkspaceError::Fenced);
        }
        for extent in &request.extents {
            validate_extent_request(extent, head, request.chunk_size)?;
        }
        let count = request
            .extents
            .len()
            .checked_add(1)
            .ok_or_else(|| WorkspaceError::Backend("too many data mutations".into()))?;
        self.hot_mutation(&request.guard, |layer, writes| {
            let first = allocate_layer_sequences(layer, count)?
                .expect("data mutation allocates a sequence")
                .0;
            let mut inode = request.inode.clone();
            inode.sequence = first;
            writes.push(put(inode_key(&inode), &inode)?);

            let mut extents = request.extents.clone();
            let mut owned_slice_count = 0_u64;
            let mut owned_bytes = 0_u64;
            for (index, extent) in extents.iter_mut().enumerate() {
                extent.sequence = first
                    .checked_add(index as u64)
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| WorkspaceError::Backend("extent sequence overflows".into()))?;
                if matches!(extent.kind, ExtentKind::Data { .. }) {
                    owned_slice_count = owned_slice_count.checked_add(1).ok_or_else(|| {
                        WorkspaceError::Backend("owned slice count overflows".into())
                    })?;
                    owned_bytes = owned_bytes.checked_add(extent.length).ok_or_else(|| {
                        WorkspaceError::Backend("owned byte count overflows".into())
                    })?;
                }
                writes.push(put(extent_key(extent), extent)?);
            }
            if owned_slice_count != 0 {
                layer.owned_slice_count = layer
                    .owned_slice_count
                    .checked_add(owned_slice_count)
                    .ok_or_else(|| WorkspaceError::Backend("owned slice count overflows".into()))?;
                layer.owned_bytes = layer
                    .owned_bytes
                    .checked_add(owned_bytes)
                    .ok_or_else(|| WorkspaceError::Backend("owned byte count overflows".into()))?;
            }
            Ok(DataMutationResult { inode, extents })
        })
        .await
    }

    async fn apply_xattr_mutation(&self, request: XattrMutation) -> Result<(), WorkspaceError> {
        validate_value(request.xattr.op, request.xattr.value.as_deref(), "xattr")?;
        if request.xattr.layer_id != request.guard.expected_head_layer_id {
            return Err(WorkspaceError::Fenced);
        }
        self.hot_mutation(&request.guard, |layer, writes| {
            let mut xattr = request.xattr.clone();
            xattr.sequence = allocate_layer_sequences(layer, 1)?
                .expect("single mutation has a sequence")
                .0;
            writes.push(put(xattr_key(&xattr), &xattr)?);
            Ok(())
        })
        .await
    }

    async fn apply_acl_mutation(&self, request: AclMutation) -> Result<(), WorkspaceError> {
        validate_value(request.acl.op, request.acl.value.as_deref(), "ACL")?;
        if request.acl.layer_id != request.guard.expected_head_layer_id {
            return Err(WorkspaceError::Fenced);
        }
        self.hot_mutation(&request.guard, |layer, writes| {
            let mut acl = request.acl.clone();
            acl.sequence = allocate_layer_sequences(layer, 1)?
                .expect("single mutation has a sequence")
                .0;
            writes.push(put(acl_key(&acl), &acl)?);
            Ok(())
        })
        .await
    }

    async fn load_layer_delta(
        &self,
        layer_id: LayerId,
    ) -> Result<CanonicalLayerDelta, WorkspaceError> {
        self.load_layer(layer_id).await?;
        self.layer_delta_unchecked(layer_id).await
    }

    async fn begin_seal(&self, request: BeginSeal) -> Result<SealJournal, WorkspaceError> {
        if request.new_head_layer_id == request.guard.expected_head_layer_id {
            return Err(WorkspaceError::CorruptMetadata(
                "seal new head must differ from old head".into(),
            ));
        }
        let now = self.now_ns().await?;
        self.update_control(|state, _| {
            checked_guard(state, &request.guard, now)?;
            if state.journals.contains_key(&request.journal_id) {
                return Err(conflict("seal journal already exists"));
            }
            let workspace = state
                .workspaces
                .get_mut(&request.guard.workspace_id)
                .ok_or(WorkspaceError::Fenced)?;
            workspace.state = WorkspaceState::Sealing;
            workspace.updated_at_ns = now;
            let layer = state
                .layers
                .get_mut(&request.guard.expected_head_layer_id)
                .ok_or(WorkspaceError::Fenced)?;
            layer.state = LayerState::Sealing;
            let journal = SealJournal {
                journal_id: request.journal_id,
                workspace_id: request.guard.workspace_id,
                old_head_layer_id: request.guard.expected_head_layer_id,
                expected_head_epoch: request.guard.expected_head_epoch,
                phase: SealPhase::Prepare,
                pending_bytes: 0,
                delta_digest: None,
                root_hash: None,
                new_head_layer_id: Some(request.new_head_layer_id),
                last_error: None,
                created_at_ns: now,
                updated_at_ns: now,
            };
            state.journals.insert(request.journal_id, journal.clone());
            Ok(journal)
        })
        .await
    }

    async fn advance_seal(&self, request: AdvanceSeal) -> Result<SealJournal, WorkspaceError> {
        let allowed = matches!(
            (request.expected_phase, request.next_phase),
            (SealPhase::Prepare, SealPhase::Quiesced)
                | (SealPhase::Quiesced, SealPhase::DataDrained)
                | (SealPhase::HeadSwitched, SealPhase::Completed)
        );
        if !allowed {
            return Err(invalid_transition(
                request.expected_phase,
                request.next_phase,
            ));
        }
        let now = self.now_ns().await?;
        self.update_control(|state, _| {
            let journal = state.journals.get_mut(&request.journal_id).ok_or_else(|| {
                WorkspaceError::Backend(format!("seal journal not found: {}", request.journal_id))
            })?;
            if journal.phase != request.expected_phase {
                return Err(invalid_transition(journal.phase, request.next_phase));
            }
            journal.phase = request.next_phase;
            if let Some(bytes) = request.pending_bytes {
                journal.pending_bytes = bytes;
            }
            journal.last_error = request.last_error.clone();
            journal.updated_at_ns = now;
            Ok(journal.clone())
        })
        .await
    }

    async fn hash_seal(&self, journal_id: JournalId) -> Result<SealJournal, WorkspaceError> {
        let initial = self.load_seal_journal(journal_id).await?;
        if initial.phase != SealPhase::DataDrained {
            return Err(invalid_transition(initial.phase, SealPhase::Hashed));
        }
        let delta = self
            .layer_delta_unchecked(initial.old_head_layer_id)
            .await?;
        let digest = delta_digest(&delta)?;
        let control = self.load_control().await?;
        let old = control
            .layers
            .get(&initial.old_head_layer_id)
            .ok_or(WorkspaceError::LayerNotFound(initial.old_head_layer_id))?;
        let parent_hash = match old.parent_layer_id {
            Some(parent) => control
                .layers
                .get(&parent)
                .and_then(|layer| layer.root_hash)
                .ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("sealed parent has no root hash".into())
                })?,
            None => [0; 32],
        };
        let root = root_hash(parent_hash, digest);
        let now = self.now_ns().await?;
        self.update_control(|state, _| {
            let journal = state.journals.get_mut(&journal_id).ok_or_else(|| {
                WorkspaceError::Backend(format!("seal journal not found: {journal_id}"))
            })?;
            if journal.phase != SealPhase::DataDrained
                || journal.old_head_layer_id != initial.old_head_layer_id
            {
                return Err(WorkspaceError::Fenced);
            }
            journal.phase = SealPhase::Hashed;
            journal.delta_digest = Some(digest);
            journal.root_hash = Some(root);
            journal.updated_at_ns = now;
            Ok(journal.clone())
        })
        .await
    }

    async fn commit_seal(&self, journal_id: JournalId) -> Result<SealResult, WorkspaceError> {
        let now = self.now_ns().await?;
        self.update_control(|state, _| {
            let journal = state.journals.get(&journal_id).cloned().ok_or_else(|| {
                WorkspaceError::Backend(format!("seal journal not found: {journal_id}"))
            })?;
            if matches!(
                journal.phase,
                SealPhase::Completed | SealPhase::HeadSwitched
            ) {
                let revision = revision_state(state, journal.old_head_layer_id)?;
                let workspace = state
                    .workspaces
                    .get(&journal.workspace_id)
                    .ok_or(WorkspaceError::WorkspaceNotFound(journal.workspace_id))?;
                if journal.phase == SealPhase::HeadSwitched {
                    let journal = state.journals.get_mut(&journal_id).expect("loaded journal");
                    journal.phase = SealPhase::Completed;
                    journal.updated_at_ns = now;
                }
                return Ok(SealResult {
                    revision,
                    new_head_layer_id: workspace.head_layer_id,
                    head_epoch: workspace.head_epoch,
                });
            }
            if journal.phase != SealPhase::Hashed {
                return Err(invalid_transition(journal.phase, SealPhase::HeadSwitched));
            }
            let digest = journal.delta_digest.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("hashed journal lacks digest".into())
            })?;
            let root = journal.root_hash.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("hashed journal lacks root hash".into())
            })?;
            let new_head = journal.new_head_layer_id.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("seal journal lacks new head".into())
            })?;
            if state.layers.contains_key(&new_head) {
                return Err(conflict("seal replacement head already exists"));
            }
            let old = state
                .layers
                .get(&journal.old_head_layer_id)
                .ok_or(WorkspaceError::Fenced)?;
            if old.state != LayerState::Sealing {
                return Err(WorkspaceError::Fenced);
            }
            let new_depth = old
                .depth
                .checked_add(1)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("layer depth overflows".into()))?;
            check_depth(new_depth)?;
            let sealed_version = u64::try_from(allocate_id_state(state, "sealed_version")?)
                .map_err(|_| WorkspaceError::CorruptMetadata("negative sealed version".into()))?;
            let old = state
                .layers
                .get_mut(&journal.old_head_layer_id)
                .expect("old layer exists");
            old.state = LayerState::Sealed;
            old.sealed_version = Some(sealed_version);
            old.delta_digest = Some(digest);
            old.root_hash = Some(root);
            old.owner_workspace_id = None;
            old.sealed_at_ns = Some(now);
            state.layers.insert(
                new_head,
                writable_layer(
                    new_head,
                    journal.old_head_layer_id,
                    new_depth,
                    journal.workspace_id,
                    now,
                ),
            );
            let new_epoch = journal
                .expected_head_epoch
                .checked_add(1)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("head epoch overflows".into()))?;
            let workspace = state
                .workspaces
                .get_mut(&journal.workspace_id)
                .ok_or(WorkspaceError::Fenced)?;
            if workspace.head_layer_id != journal.old_head_layer_id
                || workspace.head_epoch != journal.expected_head_epoch
                || workspace.state != WorkspaceState::Sealing
            {
                return Err(WorkspaceError::Fenced);
            }
            workspace.head_layer_id = new_head;
            workspace.head_epoch = new_epoch;
            workspace.state = WorkspaceState::Active;
            workspace.updated_at_ns = now;
            let revision = BaseRevision {
                layer_id: journal.old_head_layer_id,
                sealed_version,
                root_hash: root,
            };
            for lease in state.leases.values_mut() {
                if lease.workspace_id == journal.workspace_id && lease.state == LeaseState::Active {
                    lease.base_revision = revision.clone();
                    lease.updated_at_ns = now;
                }
            }
            let journal = state.journals.get_mut(&journal_id).expect("loaded journal");
            journal.phase = SealPhase::Completed;
            journal.updated_at_ns = now;
            Ok(SealResult {
                revision,
                new_head_layer_id: new_head,
                head_epoch: new_epoch,
            })
        })
        .await
    }

    async fn abort_recoverable_seal(&self, request: AbortSeal) -> Result<(), WorkspaceError> {
        let now = self.now_ns().await?;
        self.update_control(|state, _| {
            let journal = state
                .journals
                .get(&request.journal_id)
                .cloned()
                .ok_or_else(|| {
                    WorkspaceError::Backend(format!(
                        "seal journal not found: {}",
                        request.journal_id
                    ))
                })?;
            if !matches!(
                journal.phase,
                SealPhase::Prepare | SealPhase::Quiesced | SealPhase::DataDrained
            ) {
                return Err(invalid_transition(journal.phase, SealPhase::Aborted));
            }
            if let Some(layer) = state.layers.get_mut(&journal.old_head_layer_id)
                && layer.state == LayerState::Sealing
            {
                layer.state = LayerState::Writable;
            }
            if let Some(workspace) = state.workspaces.get_mut(&journal.workspace_id)
                && workspace.head_layer_id == journal.old_head_layer_id
                && workspace.head_epoch == journal.expected_head_epoch
            {
                workspace.state = WorkspaceState::Active;
                workspace.updated_at_ns = now;
            }
            let journal = state
                .journals
                .get_mut(&request.journal_id)
                .expect("loaded journal");
            journal.phase = SealPhase::Aborted;
            journal.last_error = Some(request.reason.clone());
            journal.updated_at_ns = now;
            Ok(())
        })
        .await
    }

    async fn load_seal_journal(
        &self,
        journal_id: JournalId,
    ) -> Result<SealJournal, WorkspaceError> {
        self.load_control()
            .await?
            .journals
            .get(&journal_id)
            .cloned()
            .ok_or_else(|| WorkspaceError::Backend(format!("seal journal not found: {journal_id}")))
    }

    async fn list_incomplete_seal_journals(&self) -> Result<Vec<SealJournal>, WorkspaceError> {
        let mut rows = self
            .load_control()
            .await?
            .journals
            .into_values()
            .filter(|journal| !matches!(journal.phase, SealPhase::Completed | SealPhase::Aborted))
            .collect::<Vec<_>>();
        rows.sort_by_key(|journal| journal.created_at_ns);
        Ok(rows)
    }

    async fn fast_forward_commit(
        &self,
        request: FastForwardCommit,
    ) -> Result<CommitResult, WorkspaceError> {
        let now = self.now_ns().await?;
        self.update_control(|state, _| {
            if revision_state(state, request.source_revision.layer_id)? != request.source_revision {
                return Err(commit_conflict("source revision changed"));
            }
            let target = state
                .workspaces
                .get(&request.target_workspace_id)
                .cloned()
                .ok_or(WorkspaceError::WorkspaceNotFound(
                    request.target_workspace_id,
                ))?;
            if target.head_layer_id != request.target_expected_head_layer_id
                || target.head_epoch != request.target_expected_head_epoch
                || target.state != WorkspaceState::Active
                || target.fork_base.as_ref() != Some(&request.source_fork_base)
            {
                return Err(commit_conflict("target revision changed"));
            }
            let target_head = state
                .layers
                .get(&target.head_layer_id)
                .cloned()
                .ok_or_else(|| commit_conflict("target head is not writable"))?;
            if target_head.state != LayerState::Writable
                || target_head.owner_workspace_id != Some(target.workspace_id)
                || target_head.next_sequence != 1
            {
                return Err(commit_conflict("target writable head is not empty"));
            }
            let parent = target_head
                .parent_layer_id
                .ok_or_else(|| commit_conflict("target head has no base"))?;
            if revision_state(state, parent)? != request.source_fork_base {
                return Err(commit_conflict("target base revision changed"));
            }
            if state.leases.values().any(|lease| {
                lease.workspace_id == target.workspace_id
                    && lease.writable
                    && lease.state == LeaseState::Active
                    && lease.expires_at_ns > now
            }) {
                return Err(commit_conflict("target has an active writable lease"));
            }
            if state.layers.contains_key(&request.new_head_layer_id) {
                return Err(commit_conflict("replacement head already exists"));
            }
            let source_depth = state
                .layers
                .get(&request.source_revision.layer_id)
                .ok_or(WorkspaceError::LayerNotFound(
                    request.source_revision.layer_id,
                ))?
                .depth;
            let depth = source_depth
                .checked_add(1)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("layer depth overflows".into()))?;
            check_depth(depth)?;
            state.layers.insert(
                request.new_head_layer_id,
                writable_layer(
                    request.new_head_layer_id,
                    request.source_revision.layer_id,
                    depth,
                    target.workspace_id,
                    now,
                ),
            );
            let epoch = target
                .head_epoch
                .checked_add(1)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("head epoch overflows".into()))?;
            let workspace = state
                .workspaces
                .get_mut(&target.workspace_id)
                .expect("target workspace exists");
            workspace.head_layer_id = request.new_head_layer_id;
            workspace.head_epoch = epoch;
            workspace.fork_base = Some(request.source_revision.clone());
            workspace.updated_at_ns = now;
            let old_head = state
                .layers
                .get_mut(&target.head_layer_id)
                .expect("target head exists");
            old_head.state = LayerState::Deleting;
            old_head.owner_workspace_id = None;
            Ok(CommitResult {
                revision: request.source_revision.clone(),
                target_head_layer_id: request.new_head_layer_id,
                target_head_epoch: epoch,
            })
        })
        .await
    }

    async fn mark_workspace_deleting(&self, request: MarkDeleting) -> Result<(), WorkspaceError> {
        let now = self.now_ns().await?;
        self.update_control(|state, _| {
            let active = state.leases.values().any(|lease| {
                lease.workspace_id == request.workspace_id
                    && lease.state == LeaseState::Active
                    && lease.expires_at_ns > now
            });
            if active && !request.force_fence_lease {
                return Err(WorkspaceError::Busy);
            }
            if request.force_fence_lease {
                for lease in state.leases.values_mut() {
                    if lease.workspace_id == request.workspace_id
                        && lease.state == LeaseState::Active
                    {
                        lease.state = LeaseState::Released;
                        lease.updated_at_ns = now;
                    }
                }
            }
            let workspace = state
                .workspaces
                .get_mut(&request.workspace_id)
                .ok_or(WorkspaceError::WorkspaceNotFound(request.workspace_id))?;
            if workspace.state != WorkspaceState::Active {
                return Err(WorkspaceError::WorkspaceNotFound(request.workspace_id));
            }
            workspace.state = WorkspaceState::Deleting;
            workspace.updated_at_ns = now;
            let head = workspace.head_layer_id;
            if let Some(layer) = state.layers.get_mut(&head)
                && layer.state == LayerState::Writable
            {
                layer.state = LayerState::Deleting;
                layer.owner_workspace_id = None;
            }
            Ok(())
        })
        .await
    }

    async fn record_orphan_slice(&self, request: RecordOrphanSlice) -> Result<(), WorkspaceError> {
        if request.slice_end == 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "orphan slice length must be non-zero".into(),
            ));
        }
        let now = self.now_ns().await?;
        let extent = DataExtentDelta::data(
            request.orphan_layer_id,
            1,
            0,
            0,
            request.slice_end,
            request.slice_id,
            0,
            1,
        );
        self.update_control(|state, writes| {
            if state.layers.contains_key(&request.orphan_layer_id) {
                return Err(conflict("orphan layer already exists"));
            }
            state.layers.insert(
                request.orphan_layer_id,
                LayerRecord {
                    layer_id: request.orphan_layer_id,
                    parent_layer_id: None,
                    state: LayerState::Deleting,
                    schema_version: WORKSPACE_SCHEMA_VERSION,
                    sealed_version: None,
                    delta_digest: None,
                    root_hash: None,
                    depth: 1,
                    owner_workspace_id: None,
                    next_sequence: 2,
                    owned_slice_count: 1,
                    owned_bytes: request.slice_end,
                    created_at_ns: now,
                    sealed_at_ns: None,
                },
            );
            writes.push(put(extent_key(&extent), &extent)?);
            Ok(())
        })
        .await
    }

    async fn gc_snapshot(
        &self,
        now_ns: i64,
        lease_grace_ns: u64,
    ) -> Result<GcSnapshot, WorkspaceError> {
        let state = self.load_control().await?;
        let lease_cutoff = now_ns.saturating_sub(u64_to_i64(lease_grace_ns, "lease grace")?);
        let mut roots = BTreeSet::new();
        for workspace in state.workspaces.values() {
            if workspace.state != WorkspaceState::Deleting {
                roots.insert(workspace.head_layer_id);
            }
        }
        for lease in state.leases.values() {
            if matches!(lease.state, LeaseState::Active | LeaseState::Releasing)
                && lease.expires_at_ns > lease_cutoff
            {
                roots.insert(lease.base_revision.layer_id);
            }
        }
        roots.extend(
            state
                .snapshots
                .values()
                .map(|snapshot| snapshot.revision.layer_id),
        );
        for journal in state.journals.values() {
            if !matches!(journal.phase, SealPhase::Completed | SealPhase::Aborted) {
                roots.insert(journal.old_head_layer_id);
                if let Some(head) = journal.new_head_layer_id {
                    roots.insert(head);
                }
            }
        }
        let mut layers = state.layers.into_values().collect::<Vec<_>>();
        layers.sort_by_key(|layer| (layer.created_at_ns, layer.layer_id));
        let extents: Vec<DataExtentDelta> = self.scan(b"delta/extent/".to_vec()).await?;
        let mut slice_references = Vec::new();
        for extent in extents {
            if let ExtentKind::Data {
                slice_id,
                slice_offset,
            } = extent.kind
            {
                slice_references.push(SliceReference {
                    layer_id: extent.layer_id,
                    slice_id,
                    slice_end: slice_offset.checked_add(extent.length).ok_or_else(|| {
                        WorkspaceError::CorruptMetadata("slice reference overflows".into())
                    })?,
                });
            }
        }
        Ok(GcSnapshot {
            root_layers: roots.into_iter().collect(),
            layers,
            slice_references,
        })
    }

    async fn delete_layer_metadata(
        &self,
        request: DeleteLayerMetadata,
    ) -> Result<(), WorkspaceError> {
        if request.layer_ids.is_empty() {
            return Ok(());
        }
        self.update_control(|state, _| {
            let lease_cutoff = request
                .now_ns
                .saturating_sub(u64_to_i64(request.lease_grace_ns, "lease grace")?);
            let reachable = reachable_layers(state, lease_cutoff);
            if request
                .layer_ids
                .iter()
                .any(|layer| reachable.contains(layer))
            {
                return Err(WorkspaceError::Busy);
            }
            for layer_id in &request.layer_ids {
                if let Some(layer) = state.layers.get_mut(layer_id) {
                    layer.state = LayerState::Deleting;
                    layer.owner_workspace_id = None;
                }
            }
            Ok(())
        })
        .await
    }

    async fn finalize_layer_metadata_deletion(
        &self,
        layer_ids: Vec<LayerId>,
    ) -> Result<(), WorkspaceError> {
        let mut entries = BTreeMap::<LayerId, Vec<Vec<u8>>>::new();
        for layer_id in &layer_ids {
            let mut keys = Vec::new();
            for prefix in [
                dentry_layer_prefix(*layer_id),
                inode_layer_prefix(*layer_id),
                xattr_layer_prefix(*layer_id),
                acl_layer_prefix(*layer_id),
                extent_layer_prefix(*layer_id),
            ] {
                keys.extend(
                    self.scan_entries(prefix)
                        .await?
                        .into_iter()
                        .map(|entry| entry.key),
                );
            }
            entries.insert(*layer_id, keys);
        }
        self.update_control(|state, writes| {
            for layer_id in &layer_ids {
                if state
                    .layers
                    .get(layer_id)
                    .is_some_and(|layer| layer.state != LayerState::Deleting)
                {
                    return Err(WorkspaceError::Busy);
                }
            }
            for layer_id in &layer_ids {
                state.layers.remove(layer_id);
                if let Some(keys) = entries.get(layer_id) {
                    writes.extend(keys.iter().cloned().map(|key| KvWrite::Delete { key }));
                }
            }
            Ok(())
        })
        .await
    }

    async fn install_compaction(
        &self,
        request: InstallCompaction,
    ) -> Result<CompactionResult, WorkspaceError> {
        if request.compacted_layer_id == request.replacement_head_layer_id {
            return Err(WorkspaceError::CorruptMetadata(
                "compacted and replacement head IDs must differ".into(),
            ));
        }
        for layer_id in request
            .delta
            .dentries
            .iter()
            .map(|row| row.layer_id)
            .chain(request.delta.inodes.iter().map(|row| row.layer_id))
            .chain(request.delta.xattrs.iter().map(|row| row.layer_id))
            .chain(request.delta.acls.iter().map(|row| row.layer_id))
            .chain(request.delta.extents.iter().map(|row| row.layer_id))
        {
            if layer_id != request.compacted_layer_id {
                return Err(WorkspaceError::CorruptMetadata(
                    "compaction delta contains a foreign layer ID".into(),
                ));
            }
        }
        let digest = delta_digest(&request.delta)?;
        let root = root_hash([0; 32], digest);
        let next_sequence = request
            .delta
            .dentries
            .iter()
            .map(|row| row.sequence)
            .chain(request.delta.inodes.iter().map(|row| row.sequence))
            .chain(request.delta.xattrs.iter().map(|row| row.sequence))
            .chain(request.delta.acls.iter().map(|row| row.sequence))
            .chain(request.delta.extents.iter().map(|row| row.sequence))
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| WorkspaceError::CorruptMetadata("sequence overflows".into()))?;
        let now = self.now_ns().await?;
        self.update_control(|state, writes| {
            let workspace = state
                .workspaces
                .get(&request.workspace_id)
                .cloned()
                .ok_or(WorkspaceError::WorkspaceNotFound(request.workspace_id))?;
            if workspace.head_layer_id != request.expected_head_layer_id
                || workspace.head_epoch != request.expected_head_epoch
                || workspace.state != WorkspaceState::Active
            {
                return Err(WorkspaceError::Fenced);
            }
            let head = state
                .layers
                .get(&request.expected_head_layer_id)
                .ok_or(WorkspaceError::Fenced)?;
            if head.state != LayerState::Writable
                || head.owner_workspace_id != Some(request.workspace_id)
                || head.parent_layer_id != Some(request.expected_parent_layer_id)
                || head.next_sequence != 1
            {
                return Err(WorkspaceError::Fenced);
            }
            if state.layers.contains_key(&request.compacted_layer_id)
                || state
                    .layers
                    .contains_key(&request.replacement_head_layer_id)
            {
                return Err(conflict("compaction output layer already exists"));
            }
            let sealed_version = u64::try_from(allocate_id_state(state, "sealed_version")?)
                .map_err(|_| WorkspaceError::CorruptMetadata("negative sealed version".into()))?;
            state.layers.insert(
                request.compacted_layer_id,
                LayerRecord {
                    layer_id: request.compacted_layer_id,
                    parent_layer_id: None,
                    state: LayerState::Sealed,
                    schema_version: WORKSPACE_SCHEMA_VERSION,
                    sealed_version: Some(sealed_version),
                    delta_digest: Some(digest),
                    root_hash: Some(root),
                    depth: 1,
                    owner_workspace_id: None,
                    next_sequence,
                    owned_slice_count: 0,
                    owned_bytes: 0,
                    created_at_ns: now,
                    sealed_at_ns: Some(now),
                },
            );
            state.layers.insert(
                request.replacement_head_layer_id,
                writable_layer(
                    request.replacement_head_layer_id,
                    request.compacted_layer_id,
                    2,
                    request.workspace_id,
                    now,
                ),
            );
            let epoch = workspace
                .head_epoch
                .checked_add(1)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("head epoch overflows".into()))?;
            let workspace = state
                .workspaces
                .get_mut(&request.workspace_id)
                .expect("workspace exists");
            workspace.head_layer_id = request.replacement_head_layer_id;
            workspace.head_epoch = epoch;
            workspace.fork_base = Some(BaseRevision {
                layer_id: request.compacted_layer_id,
                sealed_version,
                root_hash: root,
            });
            workspace.updated_at_ns = now;
            let revision = workspace
                .fork_base
                .clone()
                .expect("compaction installs a fork base");
            for lease in state.leases.values_mut() {
                if lease.workspace_id == request.workspace_id && lease.state == LeaseState::Active {
                    lease.base_revision = revision.clone();
                    lease.updated_at_ns = now;
                }
            }
            let old_head = state
                .layers
                .get_mut(&request.expected_head_layer_id)
                .expect("head exists");
            old_head.state = LayerState::Deleting;
            old_head.owner_workspace_id = None;

            for row in &request.delta.dentries {
                writes.push(put(dentry_key(row), row)?);
            }
            for row in &request.delta.inodes {
                writes.push(put(inode_key(row), row)?);
            }
            for row in &request.delta.xattrs {
                writes.push(put(xattr_key(row), row)?);
            }
            for row in &request.delta.acls {
                writes.push(put(acl_key(row), row)?);
            }
            for row in &request.delta.extents {
                writes.push(put(extent_key(row), row)?);
            }
            Ok(CompactionResult {
                revision: BaseRevision {
                    layer_id: request.compacted_layer_id,
                    sealed_version,
                    root_hash: root,
                },
                replacement_head_layer_id: request.replacement_head_layer_id,
                head_epoch: epoch,
            })
        })
        .await
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, WorkspaceError> {
    let payload = bincode::serialize(value)
        .map_err(|error| WorkspaceError::Backend(format!("encode workspace record: {error}")))?;
    let mut bytes = Vec::with_capacity(ENVELOPE_MAGIC.len() + payload.len());
    bytes.extend_from_slice(ENVELOPE_MAGIC);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, WorkspaceError> {
    let payload = bytes.strip_prefix(ENVELOPE_MAGIC).ok_or_else(|| {
        WorkspaceError::CorruptMetadata("workspace KV record has invalid envelope".into())
    })?;
    bincode::deserialize(payload).map_err(|error| {
        WorkspaceError::CorruptMetadata(format!("decode workspace record: {error}"))
    })
}

fn put<T: Serialize>(key: Vec<u8>, value: &T) -> Result<KvWrite, WorkspaceError> {
    Ok(KvWrite::Put {
        key,
        value: encode(value)?,
    })
}

fn hot_workspace_key(id: WorkspaceId) -> Vec<u8> {
    format!("hot/workspace/{id}").into_bytes()
}

fn hot_layer_key(id: LayerId) -> Vec<u8> {
    format!("hot/layer/{id}").into_bytes()
}

fn hot_lease_key(id: LeaseId) -> Vec<u8> {
    format!("hot/lease/{id}").into_bytes()
}

fn hot_allocator_key(name: &str) -> Vec<u8> {
    format!("hot/allocator/{name}").into_bytes()
}

fn is_hot_key(key: &[u8]) -> bool {
    key.starts_with(HOT_WORKSPACE_PREFIX)
        || key.starts_with(HOT_LAYER_PREFIX)
        || key.starts_with(HOT_LEASE_PREFIX)
        || key.starts_with(HOT_ALLOCATOR_PREFIX)
}

fn allocator_name_from_key(key: &[u8]) -> Result<String, WorkspaceError> {
    let name = key.strip_prefix(HOT_ALLOCATOR_PREFIX).ok_or_else(|| {
        WorkspaceError::CorruptMetadata("invalid hot allocator key prefix".into())
    })?;
    String::from_utf8(name.to_vec())
        .map_err(|_| WorkspaceError::CorruptMetadata("invalid hot allocator key".into()))
}

fn append_hot_diff(
    before: &ControlState,
    after: &ControlState,
    writes: &mut Vec<KvWrite>,
) -> Result<(), WorkspaceError> {
    append_map_diff(&before.workspaces, &after.workspaces, writes, |id| {
        hot_workspace_key(*id)
    })?;
    append_map_diff(&before.layers, &after.layers, writes, |id| {
        hot_layer_key(*id)
    })?;
    append_map_diff(&before.leases, &after.leases, writes, |id| {
        hot_lease_key(*id)
    })?;
    append_map_diff(&before.allocators, &after.allocators, writes, |name| {
        hot_allocator_key(name)
    })?;
    Ok(())
}

fn append_map_diff<K, V, F>(
    before: &BTreeMap<K, V>,
    after: &BTreeMap<K, V>,
    writes: &mut Vec<KvWrite>,
    key: F,
) -> Result<(), WorkspaceError>
where
    K: Ord,
    V: PartialEq + Serialize,
    F: Fn(&K) -> Vec<u8>,
{
    for (id, value) in after {
        if before.get(id) != Some(value) {
            writes.push(put(key(id), value)?);
        }
    }
    for id in before.keys() {
        if !after.contains_key(id) {
            writes.push(KvWrite::Delete { key: key(id) });
        }
    }
    Ok(())
}

fn checked_hot_guard(
    workspace: &WorkspaceRecord,
    layer: &LayerRecord,
    lease: &SnapshotLease,
    guard: &HeadGuard,
    now: i64,
) -> Result<(), WorkspaceError> {
    if workspace.workspace_id != guard.workspace_id
        || workspace.state != WorkspaceState::Active
        || workspace.head_layer_id != guard.expected_head_layer_id
        || workspace.head_epoch != guard.expected_head_epoch
        || layer.layer_id != guard.expected_head_layer_id
        || layer.state != LayerState::Writable
        || layer.owner_workspace_id != Some(guard.workspace_id)
        || lease.lease_id != guard.lease_id
        || lease.workspace_id != guard.workspace_id
        || lease.state != LeaseState::Active
        || !lease.writable
        || lease.holder_generation != guard.holder_generation
        || lease.expires_at_ns <= now
    {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
}

fn allocate_layer_sequences(
    layer: &mut LayerRecord,
    count: usize,
) -> Result<Option<(u64, u64)>, WorkspaceError> {
    if count == 0 {
        return Ok(None);
    }
    let count = u64::try_from(count)
        .map_err(|_| WorkspaceError::CorruptMetadata("sequence count overflows".into()))?;
    let first = layer.next_sequence;
    let next = first
        .checked_add(count)
        .ok_or_else(|| WorkspaceError::CorruptMetadata("sequence overflows".into()))?;
    layer.next_sequence = next;
    Ok(Some((first, next - 1)))
}

fn writable_layer(
    layer_id: LayerId,
    parent_layer_id: LayerId,
    depth: u32,
    workspace_id: WorkspaceId,
    now: i64,
) -> LayerRecord {
    LayerRecord {
        layer_id,
        parent_layer_id: Some(parent_layer_id),
        state: LayerState::Writable,
        schema_version: WORKSPACE_SCHEMA_VERSION,
        sealed_version: None,
        delta_digest: None,
        root_hash: None,
        depth,
        owner_workspace_id: Some(workspace_id),
        next_sequence: 1,
        owned_slice_count: 0,
        owned_bytes: 0,
        created_at_ns: now,
        sealed_at_ns: None,
    }
}

fn revision_state(state: &ControlState, layer_id: LayerId) -> Result<BaseRevision, WorkspaceError> {
    let layer = state
        .layers
        .get(&layer_id)
        .ok_or(WorkspaceError::LayerNotFound(layer_id))?;
    revision_from_layer(layer)
}

fn revision_from_layer(layer: &LayerRecord) -> Result<BaseRevision, WorkspaceError> {
    if layer.state != LayerState::Sealed {
        return Err(WorkspaceError::LayerNotFound(layer.layer_id));
    }
    Ok(BaseRevision {
        layer_id: layer.layer_id,
        sealed_version: layer
            .sealed_version
            .ok_or_else(|| WorkspaceError::CorruptMetadata("sealed layer has no version".into()))?,
        root_hash: layer.root_hash.ok_or_else(|| {
            WorkspaceError::CorruptMetadata("sealed layer has no root hash".into())
        })?,
    })
}

fn checked_guard(state: &ControlState, guard: &HeadGuard, now: i64) -> Result<(), WorkspaceError> {
    let Some(workspace) = state.workspaces.get(&guard.workspace_id) else {
        return Err(WorkspaceError::Fenced);
    };
    let Some(layer) = state.layers.get(&workspace.head_layer_id) else {
        return Err(WorkspaceError::Fenced);
    };
    let Some(lease) = state.leases.get(&guard.lease_id) else {
        return Err(WorkspaceError::Fenced);
    };
    if workspace.state != WorkspaceState::Active
        || workspace.head_layer_id != guard.expected_head_layer_id
        || workspace.head_epoch != guard.expected_head_epoch
        || layer.state != LayerState::Writable
        || layer.owner_workspace_id != Some(guard.workspace_id)
        || lease.workspace_id != guard.workspace_id
        || lease.state != LeaseState::Active
        || !lease.writable
        || lease.holder_generation != guard.holder_generation
        || lease.expires_at_ns <= now
    {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
}

fn allocate_id_state(state: &mut ControlState, name: &str) -> Result<i64, WorkspaceError> {
    let next = state.allocators.get_mut(name).ok_or_else(|| {
        WorkspaceError::CorruptMetadata(format!("workspace allocator {name} is missing"))
    })?;
    if *next == i64::MAX {
        return Err(WorkspaceError::CorruptMetadata(format!(
            "workspace allocator {name} is exhausted"
        )));
    }
    let allocated = *next;
    *next += 1;
    Ok(allocated)
}

fn checked_expiry(now: i64, ttl_ns: u64) -> Result<i64, WorkspaceError> {
    now.checked_add(u64_to_i64(ttl_ns, "lease TTL")?)
        .ok_or_else(|| WorkspaceError::CorruptMetadata("lease expiry overflows".into()))
}

fn check_depth(depth: u32) -> Result<(), WorkspaceError> {
    if depth > LAYER_CHAIN_HARD_LIMIT {
        return Err(WorkspaceError::LayerDepthLimit {
            depth,
            hard_limit: LAYER_CHAIN_HARD_LIMIT,
        });
    }
    Ok(())
}

fn validate_extent_request(
    extent: &DataExtentDelta,
    head: LayerId,
    chunk_size: u64,
) -> Result<(), WorkspaceError> {
    if extent.layer_id != head {
        return Err(WorkspaceError::Fenced);
    }
    extent.validate()?;
    let end = extent
        .logical_offset
        .checked_add(extent.length)
        .ok_or_else(|| WorkspaceError::CorruptMetadata("extent range overflows".into()))?;
    if end > chunk_size {
        return Err(WorkspaceError::CorruptMetadata(format!(
            "extent end {end} exceeds chunk size {chunk_size}"
        )));
    }
    Ok(())
}

fn validate_value(op: ValueOp, value: Option<&[u8]>, kind: &str) -> Result<(), WorkspaceError> {
    match (op, value) {
        (ValueOp::Put, Some(_)) | (ValueOp::Whiteout, None) => Ok(()),
        _ => Err(WorkspaceError::CorruptMetadata(format!(
            "{kind} op/payload mismatch"
        ))),
    }
}

fn invalid_transition(from: SealPhase, to: SealPhase) -> WorkspaceError {
    WorkspaceError::InvalidStateTransition {
        from: format!("{from:?}"),
        to: format!("{to:?}"),
    }
}

fn conflict(reason: &str) -> WorkspaceError {
    WorkspaceError::Conflict(ConflictDetail {
        path: Vec::new(),
        reason: reason.into(),
    })
}

fn commit_conflict(reason: &str) -> WorkspaceError {
    conflict(reason)
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64, WorkspaceError> {
    i64::try_from(value)
        .map_err(|_| WorkspaceError::CorruptMetadata(format!("{field} exceeds i64 range")))
}

fn reachable_layers(state: &ControlState, lease_cutoff: i64) -> HashSet<LayerId> {
    let mut pending = Vec::new();
    pending.extend(
        state
            .workspaces
            .values()
            .filter(|workspace| workspace.state != WorkspaceState::Deleting)
            .map(|workspace| workspace.head_layer_id),
    );
    pending.extend(
        state
            .snapshots
            .values()
            .map(|snapshot| snapshot.revision.layer_id),
    );
    pending.extend(
        state
            .leases
            .values()
            .filter(|lease| {
                matches!(lease.state, LeaseState::Active | LeaseState::Releasing)
                    && lease.expires_at_ns > lease_cutoff
            })
            .map(|lease| lease.base_revision.layer_id),
    );
    for journal in state.journals.values() {
        if !matches!(journal.phase, SealPhase::Completed | SealPhase::Aborted) {
            pending.push(journal.old_head_layer_id);
            pending.extend(journal.new_head_layer_id);
        }
    }
    let mut reachable = HashSet::new();
    while let Some(layer_id) = pending.pop() {
        if !reachable.insert(layer_id) {
            continue;
        }
        if let Some(parent) = state
            .layers
            .get(&layer_id)
            .and_then(|layer| layer.parent_layer_id)
        {
            pending.push(parent);
        }
    }
    reachable
}

fn sort_delta(delta: &mut CanonicalLayerDelta) {
    delta
        .dentries
        .sort_by(|left, right| (left.parent_ino, &left.name).cmp(&(right.parent_ino, &right.name)));
    delta.inodes.sort_by_key(|row| row.ino);
    delta
        .xattrs
        .sort_by(|left, right| (left.ino, &left.name).cmp(&(right.ino, &right.name)));
    delta
        .acls
        .sort_by_key(|row| (row.ino, row.acl_type, row.acl_id));
    delta
        .extents
        .sort_by_key(|row| (row.ino, row.chunk_index, row.sequence));
}

fn id_component(id: LayerId) -> String {
    id.to_string()
}

fn ino_component(ino: i64) -> String {
    format!("{:016x}", (ino as u64) ^ (1_u64 << 63))
}

fn u64_component(value: u64) -> String {
    format!("{value:016x}")
}

fn dentry_layer_prefix(layer: LayerId) -> Vec<u8> {
    format!("delta/dentry/{}/", id_component(layer)).into_bytes()
}

fn dentry_parent_prefix(layer: LayerId, parent_ino: i64) -> Vec<u8> {
    format!(
        "delta/dentry/{}/{}/",
        id_component(layer),
        ino_component(parent_ino)
    )
    .into_bytes()
}

fn dentry_key(row: &DentryDelta) -> Vec<u8> {
    dentry_identity_key(row.layer_id, row.parent_ino, &row.name)
}

fn dentry_identity_key(layer: LayerId, parent_ino: i64, name: &[u8]) -> Vec<u8> {
    let mut key = dentry_parent_prefix(layer, parent_ino);
    key.extend_from_slice(hex::encode(name).as_bytes());
    key
}

fn inode_layer_prefix(layer: LayerId) -> Vec<u8> {
    format!("delta/inode/{}/", id_component(layer)).into_bytes()
}

fn inode_identity_key(layer: LayerId, ino: i64) -> Vec<u8> {
    format!("delta/inode/{}/{}", id_component(layer), ino_component(ino)).into_bytes()
}

fn inode_key(row: &InodeDelta) -> Vec<u8> {
    inode_identity_key(row.layer_id, row.ino)
}

fn xattr_layer_prefix(layer: LayerId) -> Vec<u8> {
    format!("delta/xattr/{}/", id_component(layer)).into_bytes()
}

fn xattr_inode_prefix(layer: LayerId, ino: i64) -> Vec<u8> {
    format!(
        "delta/xattr/{}/{}/",
        id_component(layer),
        ino_component(ino)
    )
    .into_bytes()
}

fn xattr_key(row: &XattrDelta) -> Vec<u8> {
    xattr_identity_key(row.layer_id, row.ino, &row.name)
}

fn xattr_identity_key(layer: LayerId, ino: i64, name: &[u8]) -> Vec<u8> {
    let mut key = xattr_inode_prefix(layer, ino);
    key.extend_from_slice(hex::encode(name).as_bytes());
    key
}

fn acl_layer_prefix(layer: LayerId) -> Vec<u8> {
    format!("delta/acl/{}/", id_component(layer)).into_bytes()
}

fn acl_inode_prefix(layer: LayerId, ino: i64) -> Vec<u8> {
    format!("delta/acl/{}/{}/", id_component(layer), ino_component(ino)).into_bytes()
}

fn acl_key(row: &AclDelta) -> Vec<u8> {
    acl_identity_key(row.layer_id, row.ino, row.acl_type, row.acl_id)
}

fn acl_identity_key(layer: LayerId, ino: i64, acl_type: u8, acl_id: i64) -> Vec<u8> {
    let mut key = acl_inode_prefix(layer, ino);
    key.extend_from_slice(format!("{acl_type:02x}/{}", ino_component(acl_id)).as_bytes());
    key
}

fn extent_layer_prefix(layer: LayerId) -> Vec<u8> {
    format!("delta/extent/{}/", id_component(layer)).into_bytes()
}

fn extent_chunk_prefix(layer: LayerId, ino: i64, chunk_index: u64) -> Vec<u8> {
    format!(
        "delta/extent/{}/{}/{}/",
        id_component(layer),
        ino_component(ino),
        u64_component(chunk_index)
    )
    .into_bytes()
}

fn extent_key(row: &DataExtentDelta) -> Vec<u8> {
    let mut key = extent_chunk_prefix(row.layer_id, row.ino, row.chunk_index);
    key.extend_from_slice(u64_component(row.sequence).as_bytes());
    key
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use tokio::sync::{Barrier, Mutex};
    use uuid::Uuid;

    use super::*;
    use crate::workspace_overlay::catalog::{
        AcquireLease, AdvanceSeal, AppendDataExtent, BeginSeal, CreateVolumeRoot, CreateWorkspace,
        DentryQuery, HeadGuard, NamespaceMutation, WorkspaceStore,
    };
    use crate::workspace_overlay::ids::{JournalId, LeaseId};
    use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
    use crate::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;

    #[derive(Clone, Default)]
    struct MemoryBackend {
        records: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
        cas_checks: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
        scans: Arc<AtomicU64>,
    }

    #[async_trait]
    impl WorkspaceKvBackend for MemoryBackend {
        fn name(&self) -> &'static str {
            "workspace-memory-test"
        }

        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
            Ok(self.records.lock().await.get(key).cloned())
        }

        async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvEntry>, WorkspaceError> {
            self.scans.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .records
                .lock()
                .await
                .range(prefix.to_vec()..)
                .take_while(|(key, _)| key.starts_with(prefix))
                .map(|(key, value)| KvEntry {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect())
        }

        async fn compare_and_swap(
            &self,
            checks: &[KvCheck],
            writes: &[KvWrite],
        ) -> Result<bool, WorkspaceError> {
            self.cas_checks
                .lock()
                .await
                .push(checks.iter().map(|check| check.key.clone()).collect());
            let mut records = self.records.lock().await;
            if checks
                .iter()
                .any(|check| records.get(&check.key) != check.expected.as_ref())
            {
                return Ok(false);
            }
            for write in writes {
                match write {
                    KvWrite::Put { key, value } => {
                        records.insert(key.clone(), value.clone());
                    }
                    KvWrite::Delete { key } => {
                        records.remove(key);
                    }
                }
            }
            Ok(true)
        }

        async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
            let duration = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| WorkspaceError::Backend(error.to_string()))?;
            i64::try_from(duration.as_nanos())
                .map_err(|_| WorkspaceError::Backend("test clock overflow".into()))
        }
    }

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn create_request(offset: u128) -> CreateVolumeRoot {
        CreateVolumeRoot {
            volume_id: id(offset + 1),
            workspace_id: WorkspaceId::from_uuid(id(offset + 2)),
            root_layer_id: LayerId::from_uuid(id(offset + 3)),
            writable_layer_id: LayerId::from_uuid(id(offset + 4)),
            owner_id: Some("kv-contract".into()),
        }
    }

    async fn initialized() -> (
        KvWorkspaceStore<MemoryBackend>,
        WorkspaceRecord,
        SnapshotLease,
        HeadGuard,
    ) {
        let store = KvWorkspaceStore::new(MemoryBackend::default());
        store.initialize_workspace_schema().await.unwrap();
        let workspace = store.create_volume_root(create_request(0)).await.unwrap();
        let lease = store
            .acquire_lease(AcquireLease {
                workspace_id: workspace.workspace_id,
                lease_id: LeaseId::from_uuid(id(5)),
                holder_generation: 1,
                ttl_ns: 120_000_000_000,
            })
            .await
            .unwrap();
        let guard = HeadGuard {
            workspace_id: workspace.workspace_id,
            expected_head_layer_id: workspace.head_layer_id,
            expected_head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        };
        (store, workspace, lease, guard)
    }

    #[tokio::test]
    async fn named_lookup_and_layer_pair_loading_never_scan() {
        let (store, workspace, _lease, guard) = initialized().await;
        store
            .apply_namespace_mutation(NamespaceMutation {
                guard,
                dentries: vec![DentryDelta::put(
                    workspace.head_layer_id,
                    1,
                    b"point-lookup".to_vec(),
                    2,
                    0,
                    0,
                )],
                inodes: Vec::new(),
            })
            .await
            .unwrap();

        store.backend.scans.store(0, Ordering::Relaxed);
        let chain = store
            .load_layer_chain(workspace.head_layer_id)
            .await
            .unwrap();
        let rows = store
            .get_dentry_deltas(DentryQuery {
                layer_ids: chain.iter().map(|layer| layer.layer_id).collect(),
                parent_ino: 1,
                name: Some(b"point-lookup".to_vec()),
            })
            .await
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(store.backend.scans.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn kv_catalog_round_trips_mutations_and_full_seal() {
        let (store, workspace, _lease, guard) = initialized().await;
        let mutation = store
            .apply_namespace_mutation(NamespaceMutation {
                guard: guard.clone(),
                dentries: vec![DentryDelta::put(
                    workspace.head_layer_id,
                    1,
                    b"agent.txt".to_vec(),
                    2,
                    1,
                    0,
                )],
                inodes: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(mutation.first_sequence, Some(1));
        let extent = store
            .append_data_extent(AppendDataExtent {
                guard: guard.clone(),
                extent: DataExtentDelta::data(workspace.head_layer_id, 2, 0, 0, 4096, 99, 0, 0),
                chunk_size: 64 * 1024 * 1024,
            })
            .await
            .unwrap();
        assert_eq!(extent.sequence, 2);
        let rows = store
            .get_dentry_deltas(DentryQuery {
                layer_ids: vec![workspace.head_layer_id],
                parent_ino: 1,
                name: None,
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);

        let journal = store
            .begin_seal(BeginSeal {
                guard,
                journal_id: JournalId::from_uuid(id(6)),
                new_head_layer_id: LayerId::from_uuid(id(7)),
            })
            .await
            .unwrap();
        store
            .advance_seal(AdvanceSeal {
                journal_id: journal.journal_id,
                expected_phase: SealPhase::Prepare,
                next_phase: SealPhase::Quiesced,
                pending_bytes: None,
                last_error: None,
            })
            .await
            .unwrap();
        store
            .advance_seal(AdvanceSeal {
                journal_id: journal.journal_id,
                expected_phase: SealPhase::Quiesced,
                next_phase: SealPhase::DataDrained,
                pending_bytes: Some(0),
                last_error: None,
            })
            .await
            .unwrap();
        store.hash_seal(journal.journal_id).await.unwrap();
        let sealed = store.commit_seal(journal.journal_id).await.unwrap();
        assert_eq!(sealed.revision.layer_id, workspace.head_layer_id);
        assert_eq!(sealed.head_epoch, 1);
        assert_eq!(
            store
                .load_layer_delta(sealed.revision.layer_id)
                .await
                .unwrap()
                .extents
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn independent_kv_store_instances_use_backend_cas() {
        let backend = MemoryBackend::default();
        let probe = backend.clone();
        let store_a = Arc::new(KvWorkspaceStore::new(backend.clone()));
        let store_b = Arc::new(KvWorkspaceStore::new(backend));
        store_a.initialize_workspace_schema().await.unwrap();
        let first = store_a
            .create_volume_root(create_request(100))
            .await
            .unwrap();
        let root = store_a
            .load_layer_chain(first.head_layer_id)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let base = BaseRevision {
            layer_id: root.layer_id,
            sealed_version: root.sealed_version.unwrap(),
            root_hash: root.root_hash.unwrap(),
        };
        let second = store_b
            .create_workspace(CreateWorkspace {
                workspace_id: WorkspaceId::from_uuid(id(110)),
                head_layer_id: LayerId::from_uuid(id(111)),
                base_revision: base,
                owner_id: None,
            })
            .await
            .unwrap();
        let lease_a = store_a
            .acquire_lease(AcquireLease {
                workspace_id: first.workspace_id,
                lease_id: LeaseId::from_uuid(id(112)),
                holder_generation: 1,
                ttl_ns: 120_000_000_000,
            })
            .await
            .unwrap();
        let lease_b = store_b
            .acquire_lease(AcquireLease {
                workspace_id: second.workspace_id,
                lease_id: LeaseId::from_uuid(id(113)),
                holder_generation: 1,
                ttl_ns: 120_000_000_000,
            })
            .await
            .unwrap();
        probe.cas_checks.lock().await.clear();
        let first_head = first.head_layer_id;
        let second_head = second.head_layer_id;
        let barrier = Arc::new(Barrier::new(2));
        let writers = [
            (
                Arc::clone(&store_a),
                first,
                lease_a,
                Arc::clone(&barrier),
                b'a',
            ),
            (Arc::clone(&store_b), second, lease_b, barrier, b'b'),
        ]
        .into_iter()
        .map(|(store, workspace, lease, barrier, tag)| {
            tokio::spawn(async move {
                barrier.wait().await;
                let guard = HeadGuard {
                    workspace_id: workspace.workspace_id,
                    expected_head_layer_id: workspace.head_layer_id,
                    expected_head_epoch: workspace.head_epoch,
                    lease_id: lease.lease_id,
                    holder_generation: lease.holder_generation,
                };
                for index in 0..32_i64 {
                    store
                        .apply_namespace_mutation(NamespaceMutation {
                            guard: guard.clone(),
                            dentries: vec![DentryDelta::put(
                                workspace.head_layer_id,
                                1,
                                format!("{}-{index}", tag as char).into_bytes(),
                                1_000 + index,
                                1,
                                0,
                            )],
                            inodes: Vec::new(),
                        })
                        .await?;
                }
                Ok::<(), WorkspaceError>(())
            })
        })
        .collect::<Vec<_>>();
        for writer in writers {
            writer.await.unwrap().unwrap();
        }
        let checks = probe.cas_checks.lock().await.clone();
        assert_eq!(checks.len(), 64);
        assert!(checks.iter().all(|keys| {
            keys.len() == 3
                && !keys.iter().any(|key| key.as_slice() == CONTROL_KEY)
                && keys.iter().any(|key| key.starts_with(HOT_WORKSPACE_PREFIX))
                && keys.iter().any(|key| key.starts_with(HOT_LAYER_PREFIX))
                && keys.iter().any(|key| key.starts_with(HOT_LEASE_PREFIX))
        }));
        assert_eq!(
            store_a.load_layer(first_head).await.unwrap().next_sequence,
            33
        );
        assert_eq!(
            store_b.load_layer(second_head).await.unwrap().next_sequence,
            33
        );
    }

    #[tokio::test]
    async fn lease_heartbeat_and_allocators_do_not_cas_control() {
        let backend = MemoryBackend::default();
        let store = KvWorkspaceStore::new(backend.clone());
        store.initialize_workspace_schema().await.unwrap();
        let workspace = store.create_volume_root(create_request(200)).await.unwrap();
        let lease = store
            .acquire_lease(AcquireLease {
                workspace_id: workspace.workspace_id,
                lease_id: LeaseId::from_uuid(id(205)),
                holder_generation: 9,
                ttl_ns: 120_000_000_000,
            })
            .await
            .unwrap();
        backend.cas_checks.lock().await.clear();

        store
            .renew_lease(RenewLease {
                lease_id: lease.lease_id,
                holder_generation: lease.holder_generation,
                ttl_ns: 120_000_000_000,
            })
            .await
            .unwrap();
        assert_eq!(store.allocate_id("inode").await.unwrap(), 2);
        assert_eq!(store.allocate_id("slice").await.unwrap(), 1);

        let checks = backend.cas_checks.lock().await.clone();
        assert_eq!(checks.len(), 3);
        assert!(checks.iter().all(|keys| {
            keys.len() == 1 && keys[0].as_slice() != CONTROL_KEY && is_hot_key(&keys[0])
        }));
        assert!(checks[0][0].starts_with(HOT_LEASE_PREFIX));
        assert!(
            checks[1..]
                .iter()
                .all(|keys| keys[0].starts_with(HOT_ALLOCATOR_PREFIX))
        );
    }

    async fn remote_backend_contract<B>(
        store_a: Arc<KvWorkspaceStore<B>>,
        store_b: Arc<KvWorkspaceStore<B>>,
    ) where
        B: WorkspaceKvBackend,
    {
        store_a.initialize_workspace_schema().await.unwrap();
        let request = CreateVolumeRoot {
            volume_id: Uuid::now_v7(),
            workspace_id: WorkspaceId::new(),
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: Some("remote-contract".into()),
        };
        let workspace = store_a.create_volume_root(request).await.unwrap();
        assert_eq!(
            store_b
                .load_workspace(workspace.workspace_id)
                .await
                .unwrap(),
            workspace
        );
        let lease = store_a
            .acquire_lease(AcquireLease {
                workspace_id: workspace.workspace_id,
                lease_id: LeaseId::new(),
                holder_generation: 77,
                ttl_ns: 120_000_000_000,
            })
            .await
            .unwrap();
        let guard = HeadGuard {
            workspace_id: workspace.workspace_id,
            expected_head_layer_id: workspace.head_layer_id,
            expected_head_epoch: workspace.head_epoch,
            lease_id: lease.lease_id,
            holder_generation: lease.holder_generation,
        };
        let barrier = Arc::new(Barrier::new(2));
        let writers = [Arc::clone(&store_a), Arc::clone(&store_b)]
            .into_iter()
            .enumerate()
            .map(|(writer, store)| {
                let barrier = Arc::clone(&barrier);
                let guard = guard.clone();
                let workspace = workspace.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    for index in 0..32_i64 {
                        store
                            .apply_namespace_mutation(NamespaceMutation {
                                guard: guard.clone(),
                                dentries: vec![DentryDelta::put(
                                    workspace.head_layer_id,
                                    1,
                                    format!("writer-{writer}-{index}").into_bytes(),
                                    2_000 + writer as i64 * 100 + index,
                                    1,
                                    0,
                                )],
                                inodes: Vec::new(),
                            })
                            .await?;
                    }
                    Ok::<(), WorkspaceError>(())
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.await.unwrap().unwrap();
        }
        let rows = store_a
            .get_dentry_deltas(DentryQuery {
                layer_ids: vec![workspace.head_layer_id],
                parent_ino: 1,
                name: None,
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 64);
        let mut sequences = rows.into_iter().map(|row| row.sequence).collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=64).collect::<Vec<_>>());
        assert_eq!(
            store_b
                .load_layer(workspace.head_layer_id)
                .await
                .unwrap()
                .next_sequence,
            65
        );
    }

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_REDIS_URL"]
    async fn redis_backend_passes_distributed_catalog_contract() {
        let url = std::env::var("BREWFS_TEST_REDIS_URL")
            .expect("BREWFS_TEST_REDIS_URL must point at an isolated Redis");
        let namespace = format!("test{}", Uuid::now_v7().simple());
        let backend_a = RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap();
        let backend_b = RedisWorkspaceBackend::connect(&url, &namespace)
            .await
            .unwrap();
        remote_backend_contract(
            Arc::new(KvWorkspaceStore::new(backend_a)),
            Arc::new(KvWorkspaceStore::new(backend_b)),
        )
        .await;
    }

    #[tokio::test]
    #[ignore = "requires BREWFS_TEST_TIKV_PD_ENDPOINTS"]
    async fn tikv_backend_passes_distributed_catalog_contract() {
        let endpoints = std::env::var("BREWFS_TEST_TIKV_PD_ENDPOINTS")
            .expect("BREWFS_TEST_TIKV_PD_ENDPOINTS must list TiKV PD endpoints")
            .split(',')
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let namespace = format!("test{}", Uuid::now_v7().simple());
        let backend_a = TiKvWorkspaceBackend::connect(endpoints.clone(), &namespace)
            .await
            .unwrap();
        let backend_b = TiKvWorkspaceBackend::connect(endpoints, &namespace)
            .await
            .unwrap();
        remote_backend_contract(
            Arc::new(KvWorkspaceStore::new(backend_a)),
            Arc::new(KvWorkspaceStore::new(backend_b)),
        )
        .await;
    }
}
