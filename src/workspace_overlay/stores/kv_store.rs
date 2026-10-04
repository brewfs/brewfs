//! Backend-neutral workspace catalog stored in Redis or TiKV.
//!
//! Topology entities have independent keys and multi-key CAS transitions.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::time::sleep;

use super::kv_backend::{KvCheck, KvEntry, KvWrite, WorkspaceKvBackend};
use crate::workspace_overlay::catalog::*;
use crate::workspace_overlay::digest::{CanonicalLayerDelta, delta_digest, root_hash};
use crate::workspace_overlay::error::{ConflictDetail, WorkspaceError};
use crate::workspace_overlay::ids::{JournalId, LayerId, LeaseId, SnapshotId, WorkspaceId};
use crate::workspace_overlay::model::*;
use crate::workspace_overlay::resolver::validate_layer_chain;

const CONTROL_KEY: &[u8] = b"control";
const WORKSPACE_PREFIX: &[u8] = b"ws/";
const LAYER_PREFIX: &[u8] = b"layer/";
const LEASE_PREFIX: &[u8] = b"lease/";
const JOURNAL_PREFIX: &[u8] = b"journal/";
const SNAPSHOT_PREFIX: &[u8] = b"snapshot/";
const ALLOCATOR_PREFIX: &[u8] = b"alloc/";
const LEASE_INDEX_PREFIX: &[u8] = b"lease-id/";
const JOURNAL_INDEX_PREFIX: &[u8] = b"journal-id/";
const SNAPSHOT_NAME_PREFIX: &[u8] = b"snapshot-name/";
const LEGACY_WORKSPACE_PREFIX: &[u8] = b"hot/workspace/";
const LEGACY_LAYER_PREFIX: &[u8] = b"hot/layer/";
const LEGACY_LEASE_PREFIX: &[u8] = b"hot/lease/";
const LEGACY_SNAPSHOT_PREFIX: &[u8] = b"hot/snapshot/";
const LEGACY_ALLOCATOR_PREFIX: &[u8] = b"hot/allocator/";
const ENVELOPE_MAGIC: &[u8; 8] = b"BWSKV001";
const CAS_MAX_RETRIES: usize = 64;
const VOLUME_FORMAT: &str = "workspace-v1";
const CATALOG_FORMAT: u32 = 2;
const CONTROL_MAGIC: &[u8; 8] = b"BWSCT002";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ControlHeader {
    schema_version: u32,
    header: Option<VolumeHeader>,
    catalog_format: u32,
}

impl Default for ControlHeader {
    fn default() -> Self {
        Self {
            schema_version: WORKSPACE_SCHEMA_VERSION,
            header: None,
            catalog_format: CATALOG_FORMAT,
        }
    }
}

// bincode 按字段顺序编码结构体；迁移时保留旧记录的字段顺序。
#[derive(Clone, Debug, Serialize, Deserialize)]
struct LegacyWorkspaceRecord {
    workspace_id: WorkspaceId,
    head_layer_id: LayerId,
    head_epoch: u64,
    fork_base: Option<BaseRevision>,
    owner_id: Option<String>,
    state: WorkspaceState,
    created_at_ns: i64,
    updated_at_ns: i64,
}

impl From<LegacyWorkspaceRecord> for WorkspaceRecord {
    fn from(row: LegacyWorkspaceRecord) -> Self {
        Self {
            workspace_id: row.workspace_id,
            head_layer_id: row.head_layer_id,
            head_epoch: row.head_epoch,
            fork_base: row.fork_base,
            owner_id: row.owner_id,
            state: row.state,
            active_lease: None,
            created_at_ns: row.created_at_ns,
            updated_at_ns: row.updated_at_ns,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LegacyControlState {
    schema_version: u32,
    header: Option<VolumeHeader>,
    workspaces: BTreeMap<WorkspaceId, LegacyWorkspaceRecord>,
    layers: BTreeMap<LayerId, LayerRecord>,
    snapshots: BTreeMap<SnapshotId, SnapshotRecord>,
    leases: BTreeMap<LeaseId, SnapshotLease>,
    journals: BTreeMap<JournalId, SealJournal>,
    allocators: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MigrationState {
    header: ControlHeader,
    workspaces: BTreeMap<WorkspaceId, WorkspaceRecord>,
    layers: BTreeMap<LayerId, LayerRecord>,
    snapshots: BTreeMap<SnapshotId, SnapshotRecord>,
    leases: BTreeMap<LeaseId, SnapshotLease>,
    journals: BTreeMap<JournalId, SealJournal>,
    allocators: BTreeMap<String, i64>,
}

struct CatalogState {
    workspaces: BTreeMap<WorkspaceId, WorkspaceRecord>,
    layers: BTreeMap<LayerId, LayerRecord>,
    snapshots: BTreeMap<SnapshotId, SnapshotRecord>,
    leases: BTreeMap<LeaseId, SnapshotLease>,
    journals: BTreeMap<JournalId, SealJournal>,
}

pub struct KvWorkspaceStore<B> {
    backend: Arc<B>,
}

struct TopologyTxn<'a, B> {
    backend: &'a B,
    checks: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    writes: BTreeMap<Vec<u8>, KvWrite>,
}

impl<'a, B: WorkspaceKvBackend> TopologyTxn<'a, B> {
    fn new(backend: &'a B) -> Self {
        Self {
            backend,
            checks: BTreeMap::new(),
            writes: BTreeMap::new(),
        }
    }

    async fn read<T: DeserializeOwned>(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Option<T>, WorkspaceError> {
        if let Some(raw) = self.checks.get(&key) {
            return raw.as_deref().map(decode).transpose();
        }
        let raw = self.backend.get(&key).await?;
        let value = raw.as_deref().map(decode).transpose()?;
        self.checks.insert(key, raw);
        Ok(value)
    }

    async fn read_workspace(
        &mut self,
        id: WorkspaceId,
    ) -> Result<Option<WorkspaceRecord>, WorkspaceError> {
        self.read(workspace_key(id)).await
    }

    async fn read_layer(&mut self, id: LayerId) -> Result<Option<LayerRecord>, WorkspaceError> {
        self.read(layer_key(id)).await
    }

    async fn read_lease(
        &mut self,
        workspace: WorkspaceId,
        id: LeaseId,
    ) -> Result<Option<SnapshotLease>, WorkspaceError> {
        self.read(lease_key(workspace, id)).await
    }

    async fn read_journal(
        &mut self,
        workspace: WorkspaceId,
        id: JournalId,
    ) -> Result<Option<SealJournal>, WorkspaceError> {
        self.read(journal_key(workspace, id)).await
    }

    async fn read_snapshot(
        &mut self,
        id: SnapshotId,
    ) -> Result<Option<SnapshotRecord>, WorkspaceError> {
        self.read(snapshot_key(id)).await
    }

    async fn read_allocator(&mut self, name: &str) -> Result<Option<i64>, WorkspaceError> {
        self.read(allocator_key(name)).await
    }

    async fn read_lease_index(
        &mut self,
        id: LeaseId,
    ) -> Result<Option<WorkspaceId>, WorkspaceError> {
        self.read(lease_index_key(id)).await
    }

    async fn read_journal_index(
        &mut self,
        id: JournalId,
    ) -> Result<Option<WorkspaceId>, WorkspaceError> {
        self.read(journal_index_key(id)).await
    }

    async fn read_snapshot_name(
        &mut self,
        name: &str,
    ) -> Result<Option<SnapshotId>, WorkspaceError> {
        self.read(snapshot_name_key(name)).await
    }

    fn put<T: Serialize>(&mut self, key: Vec<u8>, value: &T) -> Result<(), WorkspaceError> {
        if !self.checks.contains_key(&key) {
            return Err(WorkspaceError::CorruptMetadata(
                "topology write has no declared read".into(),
            ));
        }
        self.writes.insert(key.clone(), put(key, value)?);
        Ok(())
    }

    fn put_workspace(&mut self, row: &WorkspaceRecord) -> Result<(), WorkspaceError> {
        self.put(workspace_key(row.workspace_id), row)
    }

    fn put_layer(&mut self, row: &LayerRecord) -> Result<(), WorkspaceError> {
        self.put(layer_key(row.layer_id), row)
    }

    fn put_lease(&mut self, row: &SnapshotLease) -> Result<(), WorkspaceError> {
        self.put(lease_key(row.workspace_id, row.lease_id), row)
    }

    fn put_journal(&mut self, row: &SealJournal) -> Result<(), WorkspaceError> {
        self.put(journal_key(row.workspace_id, row.journal_id), row)
    }

    fn put_snapshot(&mut self, row: &SnapshotRecord) -> Result<(), WorkspaceError> {
        self.put(snapshot_key(row.snapshot_id), row)
    }

    fn put_allocator(&mut self, name: &str, value: i64) -> Result<(), WorkspaceError> {
        self.put(allocator_key(name), &value)
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<(), WorkspaceError> {
        if !self.checks.contains_key(&key) {
            return Err(WorkspaceError::CorruptMetadata(
                "topology deletion has no declared read".into(),
            ));
        }
        self.writes.insert(key.clone(), KvWrite::Delete { key });
        Ok(())
    }

    fn append_write(&mut self, write: KvWrite) {
        let key = match &write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => key.clone(),
        };
        self.writes.insert(key, write);
    }

    async fn commit(self) -> Result<bool, WorkspaceError> {
        let raw = self.backend.get(CONTROL_KEY).await?;
        let control = raw.as_ref().ok_or_else(|| {
            WorkspaceError::CorruptMetadata("workspace catalog is not initialized".into())
        })?;
        if !control.starts_with(CONTROL_MAGIC) {
            return Err(WorkspaceError::CorruptMetadata(
                "catalog migration required; run brewfs workspace migrate".into(),
            ));
        }
        let mut checks = self
            .checks
            .into_iter()
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        if !checks.iter().any(|check| check.key == CONTROL_KEY) {
            checks.push(KvCheck {
                key: CONTROL_KEY.to_vec(),
                expected: raw,
            });
        }
        let writes = self.writes.into_values().collect::<Vec<_>>();
        self.backend.compare_and_swap(&checks, &writes).await
    }
}

impl<B> KvWorkspaceStore<B>
where
    B: WorkspaceKvBackend,
{
    pub fn new(backend: B) -> Self {
        Self {
            backend: Arc::new(backend),
        }
    }

    pub fn from_arc(backend: Arc<B>) -> Self {
        Self { backend }
    }

    fn topology_txn(&self) -> TopologyTxn<'_, B> {
        TopologyTxn::new(self.backend.as_ref())
    }

    async fn sealed_ancestry(
        txn: &mut TopologyTxn<'_, B>,
        root: LayerRecord,
    ) -> Result<BaseRevision, WorkspaceError> {
        let revision = revision_from_layer(&root)?;
        let mut current = root.parent_layer_id;
        let mut depth = 0;
        while let Some(id) = current {
            depth += 1;
            check_depth(depth)?;
            let parent = txn
                .read_layer(id)
                .await?
                .ok_or(WorkspaceError::LayerNotFound(id))?;
            revision_from_layer(&parent)?;
            current = parent.parent_layer_id;
        }
        Ok(revision)
    }

    async fn journal_workspace(&self, id: JournalId) -> Result<WorkspaceId, WorkspaceError> {
        self.current_control().await?;
        self.load_hot(journal_index_key(id))
            .await?
            .1
            .ok_or_else(|| WorkspaceError::Backend(format!("seal journal not found: {id}")))
    }

    async fn scan_catalog(&self) -> Result<CatalogState, WorkspaceError> {
        for attempt in 0..CAS_MAX_RETRIES {
            let state = self.scan_catalog_once().await?;
            if catalog_roots_present(&state) {
                return Ok(state);
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn scan_catalog_once(&self) -> Result<CatalogState, WorkspaceError> {
        self.current_control().await?;
        let workspaces: Vec<WorkspaceRecord> = self.scan(WORKSPACE_PREFIX.to_vec()).await?;
        let layers: Vec<LayerRecord> = self.scan(LAYER_PREFIX.to_vec()).await?;
        let snapshots: Vec<SnapshotRecord> = self.scan(SNAPSHOT_PREFIX.to_vec()).await?;
        let leases: Vec<SnapshotLease> = self.scan(LEASE_PREFIX.to_vec()).await?;
        let journals: Vec<SealJournal> = self.scan(JOURNAL_PREFIX.to_vec()).await?;
        let state = CatalogState {
            workspaces: workspaces
                .into_iter()
                .map(|row| (row.workspace_id, row))
                .collect(),
            layers: layers.into_iter().map(|row| (row.layer_id, row)).collect(),
            snapshots: snapshots
                .into_iter()
                .map(|row| (row.snapshot_id, row))
                .collect(),
            leases: leases.into_iter().map(|row| (row.lease_id, row)).collect(),
            journals: journals
                .into_iter()
                .map(|row| (row.journal_id, row))
                .collect(),
        };
        Ok(state)
    }

    async fn current_control(&self) -> Result<Option<ControlHeader>, WorkspaceError> {
        let Some(raw) = self.backend.get(CONTROL_KEY).await? else {
            return Ok(None);
        };
        if raw.starts_with(b"BWSMG002") {
            return Err(WorkspaceError::CorruptMetadata(
                "catalog migration is incomplete; run brewfs workspace migrate".into(),
            ));
        }
        if !raw.starts_with(CONTROL_MAGIC) {
            return Err(WorkspaceError::CorruptMetadata(
                "catalog migration required; run brewfs workspace migrate".into(),
            ));
        }
        let control: ControlHeader = decode_control(&raw)?;
        if control.schema_version != WORKSPACE_SCHEMA_VERSION {
            return Err(WorkspaceError::UnsupportedSchemaVersion(
                control.schema_version,
            ));
        }
        if control.catalog_format != CATALOG_FORMAT {
            return Err(WorkspaceError::CorruptMetadata(
                "unsupported workspace catalog format".into(),
            ));
        }
        Ok(Some(control))
    }

    async fn legacy_state(
        &self,
        raw: &[u8],
    ) -> Result<(MigrationState, Vec<KvCheck>), WorkspaceError> {
        let state: LegacyControlState = decode(raw)?;
        if state.schema_version != WORKSPACE_SCHEMA_VERSION {
            return Err(WorkspaceError::UnsupportedSchemaVersion(
                state.schema_version,
            ));
        }
        let mut state = MigrationState {
            header: ControlHeader {
                schema_version: state.schema_version,
                header: state.header,
                catalog_format: CATALOG_FORMAT,
            },
            workspaces: state
                .workspaces
                .into_iter()
                .map(|(id, row)| (id, row.into()))
                .collect(),
            layers: state.layers,
            snapshots: state.snapshots,
            leases: state.leases,
            journals: state.journals,
            allocators: state.allocators,
        };
        let mut checks = vec![KvCheck {
            key: CONTROL_KEY.to_vec(),
            expected: Some(raw.to_vec()),
        }];
        for entry in self.backend.scan_prefix(LEGACY_WORKSPACE_PREFIX).await? {
            let row: LegacyWorkspaceRecord = decode(&entry.value)?;
            state.workspaces.insert(row.workspace_id, row.into());
            checks.push(KvCheck {
                key: entry.key,
                expected: Some(entry.value),
            });
        }
        for entry in self.backend.scan_prefix(LEGACY_LAYER_PREFIX).await? {
            let row: LayerRecord = decode(&entry.value)?;
            state.layers.insert(row.layer_id, row);
            checks.push(KvCheck {
                key: entry.key,
                expected: Some(entry.value),
            });
        }
        for entry in self.backend.scan_prefix(LEGACY_LEASE_PREFIX).await? {
            let row: SnapshotLease = decode(&entry.value)?;
            state.leases.insert(row.lease_id, row);
            checks.push(KvCheck {
                key: entry.key,
                expected: Some(entry.value),
            });
        }
        for entry in self.backend.scan_prefix(LEGACY_SNAPSHOT_PREFIX).await? {
            let row: SnapshotRecord = decode(&entry.value)?;
            state.snapshots.insert(row.snapshot_id, row);
            checks.push(KvCheck {
                key: entry.key,
                expected: Some(entry.value),
            });
        }
        for entry in self.backend.scan_prefix(LEGACY_ALLOCATOR_PREFIX).await? {
            let name = allocator_name_from_key(&entry.key)?;
            let value: i64 = decode(&entry.value)?;
            state.allocators.insert(name, value);
            checks.push(KvCheck {
                key: entry.key,
                expected: Some(entry.value),
            });
        }
        let now = self.now_ns().await?;
        if state
            .leases
            .values()
            .any(|lease| lease.state == LeaseState::Active && lease.expires_at_ns > now)
        {
            return Err(WorkspaceError::Busy);
        }
        for lease in state.leases.values_mut() {
            if lease.state == LeaseState::Active {
                lease.state = LeaseState::Expired;
                lease.updated_at_ns = now;
            }
        }
        Ok((state, checks))
    }

    async fn stage_migration(&self, state: &MigrationState) -> Result<(), WorkspaceError> {
        let mut writes = Vec::new();
        for (id, row) in &state.workspaces {
            writes.push(put(workspace_key(*id), row)?);
        }
        for (id, row) in &state.layers {
            writes.push(put(layer_key(*id), row)?);
        }
        for (id, row) in &state.snapshots {
            writes.push(put(snapshot_key(*id), row)?);
            if let Some(name) = &row.name {
                writes.push(put(snapshot_name_key(name), id)?);
            }
        }
        for (id, row) in &state.leases {
            writes.push(put(lease_key(row.workspace_id, *id), row)?);
            writes.push(put(lease_index_key(*id), &row.workspace_id)?);
        }
        for (id, row) in &state.journals {
            writes.push(put(journal_key(row.workspace_id, *id), row)?);
            writes.push(put(journal_index_key(*id), &row.workspace_id)?);
        }
        for (name, value) in &state.allocators {
            writes.push(put(allocator_key(name), value)?);
        }
        for write in writes {
            let KvWrite::Put { key, value } = write else {
                unreachable!()
            };
            let check = KvCheck {
                key: key.clone(),
                expected: None,
            };
            if !self
                .backend
                .compare_and_swap(
                    &[check],
                    &[KvWrite::Put {
                        key: key.clone(),
                        value: value.clone(),
                    }],
                )
                .await?
                && self.backend.get(&key).await?.as_deref() != Some(value.as_slice())
            {
                return Err(WorkspaceError::CorruptMetadata(
                    "migration staging key has unexpected contents".into(),
                ));
            }
        }
        Ok(())
    }

    async fn migrate(&self) -> Result<(), WorkspaceError> {
        for attempt in 0..CAS_MAX_RETRIES {
            let raw = self.backend.get(CONTROL_KEY).await?;
            let Some(raw) = raw else {
                if self
                    .backend
                    .compare_and_swap(
                        &[KvCheck {
                            key: CONTROL_KEY.to_vec(),
                            expected: None,
                        }],
                        &[put_control(&ControlHeader::default())?],
                    )
                    .await?
                {
                    return Ok(());
                }
                retry_backoff(attempt).await;
                continue;
            };
            if raw.starts_with(CONTROL_MAGIC) {
                self.current_control().await?;
                return Ok(());
            }
            let state = if raw.starts_with(ENVELOPE_MAGIC) {
                let (state, checks) = self.legacy_state(&raw).await?;
                let marker = encode_migration(&state)?;
                let mut writes = checks
                    .iter()
                    .skip(1)
                    .map(|check| KvWrite::Delete {
                        key: check.key.clone(),
                    })
                    .collect::<Vec<_>>();
                writes.push(KvWrite::Put {
                    key: CONTROL_KEY.to_vec(),
                    value: marker,
                });
                if !self.backend.compare_and_swap(&checks, &writes).await? {
                    retry_backoff(attempt).await;
                    continue;
                }
                state
            } else if raw.starts_with(b"BWSMG002") {
                decode_migration(&raw)?
            } else {
                return Err(WorkspaceError::CorruptMetadata(
                    "invalid workspace catalog marker".into(),
                ));
            };
            self.stage_migration(&state).await?;
            let marker = encode_migration(&state)?;
            if self
                .backend
                .compare_and_swap(
                    &[KvCheck {
                        key: CONTROL_KEY.to_vec(),
                        expected: Some(marker),
                    }],
                    &[put_control(&state.header)?],
                )
                .await?
            {
                return Ok(());
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
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
        let workspace_key = workspace_key(guard.workspace_id);
        let layer_key = layer_key(guard.expected_head_layer_id);
        let lease_key = lease_key(guard.workspace_id, guard.lease_id);
        for attempt in 0..CAS_MAX_RETRIES {
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
            retry_backoff(attempt).await;
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
        self.migrate().await
    }

    async fn load_volume_header(&self) -> Result<Option<VolumeHeader>, WorkspaceError> {
        Ok(self
            .current_control()
            .await?
            .and_then(|control| control.header))
    }

    async fn load_workspace(&self, id: WorkspaceId) -> Result<WorkspaceRecord, WorkspaceError> {
        self.current_control().await?;
        self.load_hot(workspace_key(id))
            .await?
            .1
            .ok_or(WorkspaceError::WorkspaceNotFound(id))
    }

    async fn load_layer(&self, id: LayerId) -> Result<LayerRecord, WorkspaceError> {
        self.current_control().await?;
        self.load_hot(layer_key(id))
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
        self.current_control().await?;
        let key = allocator_key(name);
        for attempt in 0..CAS_MAX_RETRIES {
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
            retry_backoff(attempt).await;
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
            active_lease: None,
            created_at_ns: now,
            updated_at_ns: now,
        };
        let root_layer = LayerRecord {
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
        for attempt in 0..CAS_MAX_RETRIES {
            let raw = self.backend.get(CONTROL_KEY).await?;
            let mut control = self.current_control().await?.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("workspace catalog is not initialized".into())
            })?;
            if control.header.is_some() {
                return Err(WorkspaceError::InvalidStateTransition {
                    from: "initialized".into(),
                    to: "create-volume-root".into(),
                });
            }
            let mut txn = self.topology_txn();
            txn.checks.insert(CONTROL_KEY.to_vec(), raw);
            if txn.read_workspace(request.workspace_id).await?.is_some()
                || txn.read_layer(request.root_layer_id).await?.is_some()
                || txn.read_layer(request.writable_layer_id).await?.is_some()
            {
                return Err(conflict("volume root entity already exists"));
            }
            for name in ["inode", "slice", "sealed_version"] {
                if txn.read_allocator(name).await?.is_some() {
                    return Err(conflict("volume allocator already exists"));
                }
            }
            txn.put_workspace(&workspace)?;
            txn.put_layer(&root_layer)?;
            txn.put_layer(&writable_layer(
                request.writable_layer_id,
                request.root_layer_id,
                2,
                request.workspace_id,
                now,
            ))?;
            txn.put_allocator("inode", 2)?;
            txn.put_allocator("slice", 1)?;
            txn.put_allocator("sealed_version", 2)?;
            control.header = Some(VolumeHeader {
                volume_format: VOLUME_FORMAT.into(),
                schema_version: WORKSPACE_SCHEMA_VERSION,
                volume_id: request.volume_id,
                created_at_ns: now,
            });
            txn.append_write(put_control(&control)?);
            txn.append_write(put(inode_key(&root_inode), &root_inode)?);
            if txn.commit().await? {
                return Ok(workspace);
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn create_workspace(
        &self,
        request: CreateWorkspace,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        let now = self.now_ns().await?;
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            let base = txn
                .read_layer(request.base_revision.layer_id)
                .await?
                .ok_or(WorkspaceError::LayerNotFound(
                    request.base_revision.layer_id,
                ))?;
            let workspace_exists = txn.read_workspace(request.workspace_id).await?.is_some();
            let head_exists = txn.read_layer(request.head_layer_id).await?.is_some();
            if workspace_exists || head_exists {
                return Err(conflict("fork workspace or head already exists"));
            }
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
            txn.put_workspace(&workspace)?;
            txn.put_layer(&head)?;
            if txn.commit().await? {
                return Ok(workspace);
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceError> {
        self.current_control().await?;
        let mut rows: Vec<WorkspaceRecord> = self.scan(WORKSPACE_PREFIX.to_vec()).await?;
        rows.sort_by_key(|row| (row.created_at_ns, row.workspace_id));
        Ok(rows)
    }

    async fn create_snapshot(
        &self,
        request: CreateSnapshot,
    ) -> Result<SnapshotRecord, WorkspaceError> {
        let now = self.now_ns().await?;
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            let layer = txn
                .read_layer(request.revision.layer_id)
                .await?
                .ok_or(WorkspaceError::LayerNotFound(request.revision.layer_id))?;
            if Self::sealed_ancestry(&mut txn, layer).await? != request.revision {
                return Err(conflict("snapshot revision changed"));
            }
            if txn.read_snapshot(request.snapshot_id).await?.is_some() {
                return Err(conflict("snapshot ID already exists"));
            }
            if let Some(name) = &request.name {
                if txn.read_snapshot_name(name).await?.is_some() {
                    return Err(conflict("snapshot name already exists"));
                }
                txn.put(snapshot_name_key(name), &request.snapshot_id)?;
            }
            let snapshot = SnapshotRecord {
                snapshot_id: request.snapshot_id,
                name: request.name.clone(),
                revision: request.revision.clone(),
                owner_id: request.owner_id.clone(),
                created_at_ns: now,
            };
            txn.put_snapshot(&snapshot)?;
            if txn.commit().await? {
                return Ok(snapshot);
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn load_snapshot(&self, id: SnapshotId) -> Result<SnapshotRecord, WorkspaceError> {
        self.current_control().await?;
        self.load_hot(snapshot_key(id))
            .await?
            .1
            .ok_or(WorkspaceError::SnapshotNotFound(id))
    }

    async fn list_snapshots(&self) -> Result<Vec<SnapshotRecord>, WorkspaceError> {
        self.current_control().await?;
        let mut rows: Vec<SnapshotRecord> = self.scan(SNAPSHOT_PREFIX.to_vec()).await?;
        rows.sort_by_key(|row| (row.created_at_ns, row.snapshot_id));
        Ok(rows)
    }

    async fn delete_snapshot(&self, id: SnapshotId) -> Result<(), WorkspaceError> {
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            let snapshot = txn
                .read_snapshot(id)
                .await?
                .ok_or(WorkspaceError::SnapshotNotFound(id))?;
            if let Some(name) = &snapshot.name {
                if txn.read_snapshot_name(name).await? != Some(id) {
                    return Err(WorkspaceError::CorruptMetadata(
                        "snapshot name index disagrees with record".into(),
                    ));
                }
                txn.delete(snapshot_name_key(name))?;
            }
            txn.delete(snapshot_key(id))?;
            if txn.commit().await? {
                return Ok(());
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn acquire_lease(&self, request: AcquireLease) -> Result<SnapshotLease, WorkspaceError> {
        if request.ttl_ns == 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "lease TTL must be positive".into(),
            ));
        }
        for attempt in 0..CAS_MAX_RETRIES {
            let now = self.now_ns().await?;
            let expires = checked_expiry(now, request.ttl_ns)?;
            let mut txn = self.topology_txn();
            let mut workspace = txn
                .read_workspace(request.workspace_id)
                .await?
                .ok_or(WorkspaceError::WorkspaceNotFound(request.workspace_id))?;
            if workspace.state != WorkspaceState::Active {
                return Err(WorkspaceError::Busy);
            }
            let head = txn
                .read_layer(workspace.head_layer_id)
                .await?
                .ok_or(WorkspaceError::LayerNotFound(workspace.head_layer_id))?;
            if head.state != LayerState::Writable
                || head.owner_workspace_id != Some(request.workspace_id)
            {
                return Err(WorkspaceError::Busy);
            }
            let parent = head.parent_layer_id.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("writable head has no parent".into())
            })?;
            let base = txn
                .read_layer(parent)
                .await?
                .ok_or(WorkspaceError::LayerNotFound(parent))?;
            let base_revision = Self::sealed_ancestry(&mut txn, base).await?;
            if txn.read_lease_index(request.lease_id).await?.is_some()
                || txn
                    .read_lease(request.workspace_id, request.lease_id)
                    .await?
                    .is_some()
            {
                return Err(WorkspaceError::Busy);
            }
            if let Some(old_id) = workspace.active_lease {
                let mut old = txn.read_lease(request.workspace_id, old_id).await?;
                if old.as_ref().is_some_and(|lease| {
                    lease.state == LeaseState::Active && lease.expires_at_ns > now
                }) {
                    return Err(WorkspaceError::Busy);
                }
                if let Some(ref mut stale) = old
                    && stale.state == LeaseState::Active
                {
                    stale.state = LeaseState::Expired;
                    stale.updated_at_ns = now;
                    txn.put_lease(stale)?;
                }
            }
            workspace.active_lease = Some(request.lease_id);
            workspace.updated_at_ns = now;
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
            txn.put_workspace(&workspace)?;
            txn.put_lease(&lease)?;
            txn.put(lease_index_key(request.lease_id), &request.workspace_id)?;
            if txn.commit().await? {
                return Ok(lease);
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn renew_lease(&self, request: RenewLease) -> Result<SnapshotLease, WorkspaceError> {
        if request.ttl_ns == 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "lease TTL must be positive".into(),
            ));
        }
        self.current_control().await?;
        let workspace_id: WorkspaceId = self
            .load_hot(lease_index_key(request.lease_id))
            .await?
            .1
            .ok_or(WorkspaceError::Fenced)?;
        let key = lease_key(workspace_id, request.lease_id);
        for attempt in 0..CAS_MAX_RETRIES {
            let (values, now) = self
                .backend
                .get_many_with_time(std::slice::from_ref(&key))
                .await?;
            let raw = values
                .into_iter()
                .next()
                .ok_or_else(|| WorkspaceError::Backend("lease lookup returned no values".into()))?;
            let lease: Option<SnapshotLease> = raw.as_deref().map(decode).transpose()?;
            let mut lease = lease.ok_or(WorkspaceError::Fenced)?;
            if lease.holder_generation != request.holder_generation
                || lease.state != LeaseState::Active
                || lease.expires_at_ns <= now
            {
                return Err(WorkspaceError::Fenced);
            }
            lease.expires_at_ns = checked_expiry(now, request.ttl_ns)?;
            lease.updated_at_ns = now;
            let writes = [put(key.clone(), &lease)?];
            let checks = [KvCheck {
                key: key.clone(),
                expected: raw,
            }];
            if self.backend.compare_and_swap(&checks, &writes).await? {
                return Ok(lease);
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn release_lease(&self, request: ReleaseLease) -> Result<(), WorkspaceError> {
        let now = self.now_ns().await?;
        self.current_control().await?;
        let workspace_id: WorkspaceId = self
            .load_hot(lease_index_key(request.lease_id))
            .await?
            .1
            .ok_or(WorkspaceError::Fenced)?;
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            let mut lease = txn
                .read_lease(workspace_id, request.lease_id)
                .await?
                .ok_or(WorkspaceError::Fenced)?;
            if lease.holder_generation != request.holder_generation
                || lease.state != LeaseState::Active
            {
                return Err(WorkspaceError::Fenced);
            }
            let mut workspace = txn
                .read_workspace(workspace_id)
                .await?
                .ok_or(WorkspaceError::WorkspaceNotFound(workspace_id))?;
            if workspace.active_lease == Some(request.lease_id) {
                workspace.active_lease = None;
                workspace.updated_at_ns = now;
                txn.put_workspace(&workspace)?;
            }
            lease.state = LeaseState::Released;
            lease.updated_at_ns = now;
            txn.put_lease(&lease)?;
            if txn.commit().await? {
                return Ok(());
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn reap_expired_leases(&self) -> Result<u64, WorkspaceError> {
        let now = self.now_ns().await?;
        self.current_control().await?;
        let candidates: Vec<SnapshotLease> = self.scan(LEASE_PREFIX.to_vec()).await?;
        let mut count = 0;
        for candidate in candidates {
            if candidate.state != LeaseState::Active || candidate.expires_at_ns > now {
                continue;
            }
            for attempt in 0..CAS_MAX_RETRIES {
                let mut txn = self.topology_txn();
                let Some(mut lease) = txn
                    .read_lease(candidate.workspace_id, candidate.lease_id)
                    .await?
                else {
                    break;
                };
                if lease.state != LeaseState::Active || lease.expires_at_ns > now {
                    break;
                }
                let mut workspace = txn
                    .read_workspace(candidate.workspace_id)
                    .await?
                    .ok_or(WorkspaceError::WorkspaceNotFound(candidate.workspace_id))?;
                if workspace.active_lease == Some(lease.lease_id) {
                    workspace.active_lease = None;
                    workspace.updated_at_ns = now;
                    txn.put_workspace(&workspace)?;
                }
                lease.state = LeaseState::Expired;
                lease.updated_at_ns = now;
                txn.put_lease(&lease)?;
                if txn.commit().await? {
                    count += 1;
                    break;
                }
                retry_backoff(attempt).await;
                if attempt + 1 == CAS_MAX_RETRIES {
                    return Err(WorkspaceError::Busy);
                }
            }
        }
        Ok(count)
    }

    async fn list_leases(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<SnapshotLease>, WorkspaceError> {
        self.current_control().await?;
        let mut rows: Vec<SnapshotLease> = self.scan(lease_prefix(workspace_id)).await?;
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
        if request.xattr.layer_id != request.guard.expected_head_layer_id
            || request.inode.layer_id != request.guard.expected_head_layer_id
            || request.inode.ino != request.xattr.ino
        {
            return Err(WorkspaceError::Fenced);
        }
        self.hot_mutation(&request.guard, |layer, writes| {
            let mut xattr = request.xattr.clone();
            let mut inode = request.inode.clone();
            let range =
                allocate_layer_sequences(layer, 2)?.expect("xattr mutation has a sequence range");
            xattr.sequence = range.0;
            inode.sequence = range.1;
            writes.push(put(xattr_key(&xattr), &xattr)?);
            writes.push(put(inode_key(&inode), &inode)?);
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
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            let mut workspace = txn
                .read_workspace(request.guard.workspace_id)
                .await?
                .ok_or(WorkspaceError::Fenced)?;
            let mut layer = txn
                .read_layer(request.guard.expected_head_layer_id)
                .await?
                .ok_or(WorkspaceError::Fenced)?;
            let lease = txn
                .read_lease(request.guard.workspace_id, request.guard.lease_id)
                .await?
                .ok_or(WorkspaceError::Fenced)?;
            checked_hot_guard(&workspace, &layer, &lease, &request.guard, now)?;
            if txn.read_journal_index(request.journal_id).await?.is_some()
                || txn
                    .read_journal(request.guard.workspace_id, request.journal_id)
                    .await?
                    .is_some()
            {
                return Err(conflict("seal journal already exists"));
            }
            let parent = layer.parent_layer_id.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("writable head has no parent".into())
            })?;
            let base = txn
                .read_layer(parent)
                .await?
                .ok_or(WorkspaceError::LayerNotFound(parent))?;
            Self::sealed_ancestry(&mut txn, base).await?;
            workspace.state = WorkspaceState::Sealing;
            workspace.updated_at_ns = now;
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
            txn.put_workspace(&workspace)?;
            txn.put_layer(&layer)?;
            txn.put_journal(&journal)?;
            txn.put(
                journal_index_key(request.journal_id),
                &request.guard.workspace_id,
            )?;
            if txn.commit().await? {
                return Ok(journal);
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
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
        let workspace_id = self.journal_workspace(request.journal_id).await?;
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            let mut journal = txn
                .read_journal(workspace_id, request.journal_id)
                .await?
                .ok_or_else(|| {
                    WorkspaceError::Backend(format!(
                        "seal journal not found: {}",
                        request.journal_id
                    ))
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
            txn.put_journal(&journal)?;
            if txn.commit().await? {
                return Ok(journal);
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
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
        let old = self.load_layer(initial.old_head_layer_id).await?;
        let parent_hash = match old.parent_layer_id {
            Some(parent) => self.load_layer(parent).await?.root_hash.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("sealed parent has no root hash".into())
            })?,
            None => [0; 32],
        };
        let root = root_hash(parent_hash, digest);
        let now = self.now_ns().await?;
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            let mut journal = txn
                .read_journal(initial.workspace_id, journal_id)
                .await?
                .ok_or(WorkspaceError::Fenced)?;
            if journal.phase != SealPhase::DataDrained
                || journal.old_head_layer_id != initial.old_head_layer_id
            {
                return Err(WorkspaceError::Fenced);
            }
            let current = txn
                .read_layer(initial.old_head_layer_id)
                .await?
                .ok_or(WorkspaceError::Fenced)?;
            if current != old {
                retry_backoff(attempt).await;
                continue;
            }
            if let Some(parent) = old.parent_layer_id {
                txn.read_layer(parent).await?;
            }
            journal.phase = SealPhase::Hashed;
            journal.delta_digest = Some(digest);
            journal.root_hash = Some(root);
            journal.updated_at_ns = now;
            txn.put_journal(&journal)?;
            if txn.commit().await? {
                return Ok(journal);
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn commit_seal(&self, journal_id: JournalId) -> Result<SealResult, WorkspaceError> {
        let now = self.now_ns().await?;
        let workspace_id = self.journal_workspace(journal_id).await?;
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            let mut journal = txn
                .read_journal(workspace_id, journal_id)
                .await?
                .ok_or_else(|| {
                    WorkspaceError::Backend(format!("seal journal not found: {journal_id}"))
                })?;
            let mut old = txn
                .read_layer(journal.old_head_layer_id)
                .await?
                .ok_or(WorkspaceError::Fenced)?;
            let mut workspace = txn
                .read_workspace(workspace_id)
                .await?
                .ok_or(WorkspaceError::Fenced)?;
            if matches!(
                journal.phase,
                SealPhase::Completed | SealPhase::HeadSwitched
            ) {
                let revision = revision_from_layer(&old)?;
                if journal.phase == SealPhase::HeadSwitched {
                    journal.phase = SealPhase::Completed;
                    journal.updated_at_ns = now;
                    txn.put_journal(&journal)?;
                    if !txn.commit().await? {
                        retry_backoff(attempt).await;
                        continue;
                    }
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
            if txn.read_layer(new_head).await?.is_some() {
                return Err(conflict("seal replacement head already exists"));
            }
            if old.state != LayerState::Sealing
                || workspace.head_layer_id != old.layer_id
                || workspace.head_epoch != journal.expected_head_epoch
                || workspace.state != WorkspaceState::Sealing
            {
                return Err(WorkspaceError::Fenced);
            }
            let new_depth = old
                .depth
                .checked_add(1)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("layer depth overflows".into()))?;
            check_depth(new_depth)?;
            let next = txn.read_allocator("sealed_version").await?.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("sealed version allocator missing".into())
            })?;
            let sealed_version = u64::try_from(next)
                .map_err(|_| WorkspaceError::CorruptMetadata("negative sealed version".into()))?;
            let updated = next
                .checked_add(1)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("allocator overflows".into()))?;
            txn.put_allocator("sealed_version", updated)?;
            old.state = LayerState::Sealed;
            old.sealed_version = Some(sealed_version);
            old.delta_digest = Some(digest);
            old.root_hash = Some(root);
            old.owner_workspace_id = None;
            old.sealed_at_ns = Some(now);
            txn.put_layer(&old)?;
            txn.put_layer(&writable_layer(
                new_head,
                old.layer_id,
                new_depth,
                workspace_id,
                now,
            ))?;
            let new_epoch = workspace
                .head_epoch
                .checked_add(1)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("head epoch overflows".into()))?;
            workspace.head_layer_id = new_head;
            workspace.head_epoch = new_epoch;
            workspace.state = WorkspaceState::Active;
            workspace.updated_at_ns = now;
            txn.put_workspace(&workspace)?;
            let revision = BaseRevision {
                layer_id: old.layer_id,
                sealed_version,
                root_hash: root,
            };
            if let Some(lease_id) = workspace.active_lease
                && let Some(mut lease) = txn.read_lease(workspace_id, lease_id).await?
                && lease.state == LeaseState::Active
            {
                lease.base_revision = revision.clone();
                lease.updated_at_ns = now;
                txn.put_lease(&lease)?;
            }
            journal.phase = SealPhase::Completed;
            journal.updated_at_ns = now;
            txn.put_journal(&journal)?;
            if txn.commit().await? {
                return Ok(SealResult {
                    revision,
                    new_head_layer_id: new_head,
                    head_epoch: new_epoch,
                });
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn abort_recoverable_seal(&self, request: AbortSeal) -> Result<(), WorkspaceError> {
        let now = self.now_ns().await?;
        let workspace_id = self.journal_workspace(request.journal_id).await?;
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            let mut journal = txn
                .read_journal(workspace_id, request.journal_id)
                .await?
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
            if let Some(mut layer) = txn.read_layer(journal.old_head_layer_id).await?
                && layer.state == LayerState::Sealing
            {
                layer.state = LayerState::Writable;
                txn.put_layer(&layer)?;
            }
            if let Some(mut workspace) = txn.read_workspace(workspace_id).await?
                && workspace.head_layer_id == journal.old_head_layer_id
                && workspace.head_epoch == journal.expected_head_epoch
            {
                workspace.state = WorkspaceState::Active;
                workspace.updated_at_ns = now;
                txn.put_workspace(&workspace)?;
            }
            journal.phase = SealPhase::Aborted;
            journal.last_error = Some(request.reason.clone());
            journal.updated_at_ns = now;
            txn.put_journal(&journal)?;
            if txn.commit().await? {
                return Ok(());
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn load_seal_journal(
        &self,
        journal_id: JournalId,
    ) -> Result<SealJournal, WorkspaceError> {
        let workspace_id = self.journal_workspace(journal_id).await?;
        self.load_hot(journal_key(workspace_id, journal_id))
            .await?
            .1
            .ok_or_else(|| WorkspaceError::Backend(format!("seal journal not found: {journal_id}")))
    }

    async fn list_incomplete_seal_journals(&self) -> Result<Vec<SealJournal>, WorkspaceError> {
        self.current_control().await?;
        let mut rows: Vec<SealJournal> = self.scan(JOURNAL_PREFIX.to_vec()).await?;
        rows.retain(|journal| !matches!(journal.phase, SealPhase::Completed | SealPhase::Aborted));
        rows.sort_by_key(|journal| journal.created_at_ns);
        Ok(rows)
    }

    async fn list_seal_journals(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<SealJournal>, WorkspaceError> {
        self.current_control().await?;
        let mut rows: Vec<SealJournal> = self.scan(journal_prefix(workspace_id)).await?;
        rows.sort_by_key(|journal| (journal.created_at_ns, journal.journal_id));
        Ok(rows)
    }

    async fn fast_forward_commit(
        &self,
        request: FastForwardCommit,
    ) -> Result<CommitResult, WorkspaceError> {
        let now = self.now_ns().await?;
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            let source = txn
                .read_layer(request.source_revision.layer_id)
                .await?
                .ok_or(WorkspaceError::LayerNotFound(
                    request.source_revision.layer_id,
                ))?;
            if Self::sealed_ancestry(&mut txn, source.clone()).await? != request.source_revision {
                return Err(commit_conflict("source revision changed"));
            }
            let mut target = txn
                .read_workspace(request.target_workspace_id)
                .await?
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
            let mut old_head = txn
                .read_layer(target.head_layer_id)
                .await?
                .ok_or_else(|| commit_conflict("target head is not writable"))?;
            if old_head.state != LayerState::Writable
                || old_head.owner_workspace_id != Some(target.workspace_id)
                || old_head.next_sequence != 1
            {
                return Err(commit_conflict("target writable head is not empty"));
            }
            let parent = old_head
                .parent_layer_id
                .ok_or_else(|| commit_conflict("target head has no base"))?;
            let base = txn
                .read_layer(parent)
                .await?
                .ok_or(WorkspaceError::LayerNotFound(parent))?;
            if Self::sealed_ancestry(&mut txn, base).await? != request.source_fork_base {
                return Err(commit_conflict("target base revision changed"));
            }
            if let Some(id) = target.active_lease {
                if txn
                    .read_lease(target.workspace_id, id)
                    .await?
                    .is_some_and(|lease| {
                        lease.state == LeaseState::Active && lease.expires_at_ns > now
                    })
                {
                    return Err(commit_conflict("target has an active writable lease"));
                }
                target.active_lease = None;
            }
            if txn.read_layer(request.new_head_layer_id).await?.is_some() {
                return Err(commit_conflict("replacement head already exists"));
            }
            let depth = source
                .depth
                .checked_add(1)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("layer depth overflows".into()))?;
            check_depth(depth)?;
            txn.put_layer(&writable_layer(
                request.new_head_layer_id,
                source.layer_id,
                depth,
                target.workspace_id,
                now,
            ))?;
            let epoch = target
                .head_epoch
                .checked_add(1)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("head epoch overflows".into()))?;
            target.head_layer_id = request.new_head_layer_id;
            target.head_epoch = epoch;
            target.fork_base = Some(request.source_revision.clone());
            target.updated_at_ns = now;
            txn.put_workspace(&target)?;
            old_head.state = LayerState::Deleting;
            old_head.owner_workspace_id = None;
            txn.put_layer(&old_head)?;
            if txn.commit().await? {
                return Ok(CommitResult {
                    revision: request.source_revision.clone(),
                    target_head_layer_id: request.new_head_layer_id,
                    target_head_epoch: epoch,
                });
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn mark_workspace_deleting(&self, request: MarkDeleting) -> Result<(), WorkspaceError> {
        let now = self.now_ns().await?;
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            let mut workspace = txn
                .read_workspace(request.workspace_id)
                .await?
                .ok_or(WorkspaceError::WorkspaceNotFound(request.workspace_id))?;
            if workspace.state != WorkspaceState::Active {
                return Err(WorkspaceError::WorkspaceNotFound(request.workspace_id));
            }
            let mut head = txn.read_layer(workspace.head_layer_id).await?;
            if let Some(id) = workspace.active_lease {
                if let Some(mut lease) = txn.read_lease(request.workspace_id, id).await? {
                    if lease.state == LeaseState::Active
                        && lease.expires_at_ns > now
                        && !request.force_fence_lease
                    {
                        return Err(WorkspaceError::Busy);
                    }
                    if lease.state == LeaseState::Active {
                        lease.state = if request.force_fence_lease {
                            LeaseState::Released
                        } else {
                            LeaseState::Expired
                        };
                        lease.updated_at_ns = now;
                        txn.put_lease(&lease)?;
                    }
                }
                workspace.active_lease = None;
            }
            workspace.state = WorkspaceState::Deleting;
            workspace.updated_at_ns = now;
            txn.put_workspace(&workspace)?;
            if let Some(ref mut layer) = head
                && layer.state == LayerState::Writable
            {
                layer.state = LayerState::Deleting;
                layer.owner_workspace_id = None;
                txn.put_layer(layer)?;
            }
            if txn.commit().await? {
                return Ok(());
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
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
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            if txn.read_layer(request.orphan_layer_id).await?.is_some() {
                return Err(conflict("orphan layer already exists"));
            }
            let row = LayerRecord {
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
            };
            txn.put_layer(&row)?;
            txn.append_write(put(extent_key(&extent), &extent)?);
            if txn.commit().await? {
                return Ok(());
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn gc_snapshot(
        &self,
        now_ns: i64,
        lease_grace_ns: u64,
    ) -> Result<GcSnapshot, WorkspaceError> {
        let state = self.scan_catalog().await?;
        let lease_cutoff = now_ns.saturating_sub(u64_to_i64(lease_grace_ns, "lease grace")?);
        let mut roots = BTreeSet::new();
        for workspace in state.workspaces.values() {
            if workspace.state != WorkspaceState::Deleting {
                roots.insert(workspace.head_layer_id);
            }
        }
        for lease in state.leases.values() {
            let protected = match lease.state {
                LeaseState::Active | LeaseState::Releasing | LeaseState::Expired => {
                    lease.expires_at_ns > lease_cutoff
                }
                LeaseState::Released => lease.updated_at_ns > lease_cutoff,
            };
            if protected {
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
        let cutoff = request
            .now_ns
            .saturating_sub(u64_to_i64(request.lease_grace_ns, "lease grace")?);
        for id in request.layer_ids {
            let snapshot = self.scan_catalog().await?;
            if reachable_layers(&snapshot, cutoff).contains(&id) {
                return Err(WorkspaceError::Busy);
            }
            for attempt in 0..CAS_MAX_RETRIES {
                let mut txn = self.topology_txn();
                let Some(mut layer) = txn.read_layer(id).await? else {
                    break;
                };
                if layer.state == LayerState::Deleting {
                    break;
                }
                if layer.state != LayerState::Sealed {
                    return Err(WorkspaceError::Busy);
                }
                layer.state = LayerState::Deleting;
                txn.put_layer(&layer)?;
                if txn.commit().await? {
                    break;
                }
                retry_backoff(attempt).await;
                if attempt + 1 == CAS_MAX_RETRIES {
                    return Err(WorkspaceError::Busy);
                }
            }
            let verify = self.scan_catalog().await?;
            if reachable_layers(&verify, cutoff).contains(&id) {
                for attempt in 0..CAS_MAX_RETRIES {
                    let mut txn = self.topology_txn();
                    let Some(mut layer) = txn.read_layer(id).await? else {
                        return Err(WorkspaceError::Busy);
                    };
                    if layer.state != LayerState::Deleting {
                        break;
                    }
                    layer.state = LayerState::Sealed;
                    txn.put_layer(&layer)?;
                    if txn.commit().await? {
                        break;
                    }
                    retry_backoff(attempt).await;
                    if attempt + 1 == CAS_MAX_RETRIES {
                        return Err(WorkspaceError::Busy);
                    }
                }
                return Err(WorkspaceError::Busy);
            }
        }
        Ok(())
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
        for id in layer_ids {
            let state = self.scan_catalog().await?;
            let reachable = reachable_layers(&state, self.now_ns().await?);
            if reachable.contains(&id) {
                return Err(WorkspaceError::Busy);
            }
            for attempt in 0..CAS_MAX_RETRIES {
                let mut txn = self.topology_txn();
                let Some(layer) = txn.read_layer(id).await? else {
                    break;
                };
                if layer.state != LayerState::Deleting {
                    return Err(WorkspaceError::Busy);
                }
                txn.delete(layer_key(id))?;
                for key in entries.get(&id).into_iter().flatten() {
                    txn.append_write(KvWrite::Delete { key: key.clone() });
                }
                if txn.commit().await? {
                    break;
                }
                retry_backoff(attempt).await;
                if attempt + 1 == CAS_MAX_RETRIES {
                    return Err(WorkspaceError::Busy);
                }
            }
        }
        Ok(())
    }

    async fn prune_terminal_records(
        &self,
        now_ns: i64,
        grace_ns: u64,
    ) -> Result<(), WorkspaceError> {
        let cutoff = now_ns.saturating_sub(u64_to_i64(grace_ns, "terminal record grace")?);
        let leases: Vec<SnapshotLease> = self.scan(LEASE_PREFIX.to_vec()).await?;
        for candidate in leases {
            if !matches!(candidate.state, LeaseState::Released | LeaseState::Expired)
                || candidate.updated_at_ns > cutoff
            {
                continue;
            }
            for attempt in 0..CAS_MAX_RETRIES {
                let mut txn = self.topology_txn();
                let Some(lease) = txn
                    .read_lease(candidate.workspace_id, candidate.lease_id)
                    .await?
                else {
                    break;
                };
                if !matches!(lease.state, LeaseState::Released | LeaseState::Expired)
                    || lease.updated_at_ns > cutoff
                {
                    break;
                }
                let workspace = txn.read_workspace(lease.workspace_id).await?;
                if workspace
                    .as_ref()
                    .is_some_and(|row| row.active_lease == Some(lease.lease_id))
                {
                    break;
                }
                txn.read_lease_index(lease.lease_id).await?;
                txn.delete(lease_key(lease.workspace_id, lease.lease_id))?;
                txn.delete(lease_index_key(lease.lease_id))?;
                if txn.commit().await? {
                    break;
                }
                retry_backoff(attempt).await;
                if attempt + 1 == CAS_MAX_RETRIES {
                    return Err(WorkspaceError::Busy);
                }
            }
        }
        let journals: Vec<SealJournal> = self.scan(JOURNAL_PREFIX.to_vec()).await?;
        let mut latest = BTreeMap::<WorkspaceId, (i64, JournalId)>::new();
        for journal in &journals {
            if matches!(journal.phase, SealPhase::Completed | SealPhase::Aborted) {
                let candidate = (journal.created_at_ns, journal.journal_id);
                if latest
                    .get(&journal.workspace_id)
                    .is_none_or(|current| candidate > *current)
                {
                    latest.insert(journal.workspace_id, candidate);
                }
            }
        }
        for candidate in journals {
            if !matches!(candidate.phase, SealPhase::Completed | SealPhase::Aborted)
                || candidate.updated_at_ns > cutoff
                || latest
                    .get(&candidate.workspace_id)
                    .is_some_and(|latest| latest.1 == candidate.journal_id)
            {
                continue;
            }
            for attempt in 0..CAS_MAX_RETRIES {
                let mut txn = self.topology_txn();
                let Some(journal) = txn
                    .read_journal(candidate.workspace_id, candidate.journal_id)
                    .await?
                else {
                    break;
                };
                if !matches!(journal.phase, SealPhase::Completed | SealPhase::Aborted)
                    || journal.updated_at_ns > cutoff
                {
                    break;
                }
                txn.read_journal_index(journal.journal_id).await?;
                txn.delete(journal_key(journal.workspace_id, journal.journal_id))?;
                txn.delete(journal_index_key(journal.journal_id))?;
                if txn.commit().await? {
                    break;
                }
                retry_backoff(attempt).await;
                if attempt + 1 == CAS_MAX_RETRIES {
                    return Err(WorkspaceError::Busy);
                }
            }
        }
        Ok(())
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
        for attempt in 0..CAS_MAX_RETRIES {
            let mut txn = self.topology_txn();
            let mut workspace = txn
                .read_workspace(request.workspace_id)
                .await?
                .ok_or(WorkspaceError::WorkspaceNotFound(request.workspace_id))?;
            if workspace.head_layer_id != request.expected_head_layer_id
                || workspace.head_epoch != request.expected_head_epoch
                || workspace.state != WorkspaceState::Active
            {
                return Err(WorkspaceError::Fenced);
            }
            let mut head = txn
                .read_layer(request.expected_head_layer_id)
                .await?
                .ok_or(WorkspaceError::Fenced)?;
            if head.state != LayerState::Writable
                || head.owner_workspace_id != Some(request.workspace_id)
                || head.parent_layer_id != Some(request.expected_parent_layer_id)
                || head.next_sequence != 1
            {
                return Err(WorkspaceError::Fenced);
            }
            let parent = txn
                .read_layer(request.expected_parent_layer_id)
                .await?
                .ok_or(WorkspaceError::Fenced)?;
            revision_from_layer(&parent)?;
            if txn.read_layer(request.compacted_layer_id).await?.is_some()
                || txn
                    .read_layer(request.replacement_head_layer_id)
                    .await?
                    .is_some()
            {
                return Err(conflict("compaction output layer already exists"));
            }
            let next = txn.read_allocator("sealed_version").await?.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("sealed version allocator missing".into())
            })?;
            let sealed_version = u64::try_from(next)
                .map_err(|_| WorkspaceError::CorruptMetadata("negative sealed version".into()))?;
            txn.put_allocator(
                "sealed_version",
                next.checked_add(1)
                    .ok_or_else(|| WorkspaceError::CorruptMetadata("allocator overflows".into()))?,
            )?;
            let compacted = LayerRecord {
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
            };
            txn.put_layer(&compacted)?;
            txn.put_layer(&writable_layer(
                request.replacement_head_layer_id,
                request.compacted_layer_id,
                2,
                request.workspace_id,
                now,
            ))?;
            let epoch = workspace
                .head_epoch
                .checked_add(1)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("head epoch overflows".into()))?;
            workspace.head_layer_id = request.replacement_head_layer_id;
            workspace.head_epoch = epoch;
            let revision = BaseRevision {
                layer_id: request.compacted_layer_id,
                sealed_version,
                root_hash: root,
            };
            workspace.fork_base = Some(revision.clone());
            workspace.updated_at_ns = now;
            if let Some(id) = workspace.active_lease
                && let Some(mut lease) = txn.read_lease(request.workspace_id, id).await?
                && lease.state == LeaseState::Active
            {
                lease.base_revision = revision.clone();
                lease.updated_at_ns = now;
                txn.put_lease(&lease)?;
            }
            txn.put_workspace(&workspace)?;
            head.state = LayerState::Deleting;
            head.owner_workspace_id = None;
            txn.put_layer(&head)?;
            for row in &request.delta.dentries {
                txn.append_write(put(dentry_key(row), row)?);
            }
            for row in &request.delta.inodes {
                txn.append_write(put(inode_key(row), row)?);
            }
            for row in &request.delta.xattrs {
                txn.append_write(put(xattr_key(row), row)?);
            }
            for row in &request.delta.acls {
                txn.append_write(put(acl_key(row), row)?);
            }
            for row in &request.delta.extents {
                txn.append_write(put(extent_key(row), row)?);
            }
            if txn.commit().await? {
                return Ok(CompactionResult {
                    revision,
                    replacement_head_layer_id: request.replacement_head_layer_id,
                    head_epoch: epoch,
                });
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
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

fn workspace_key(id: WorkspaceId) -> Vec<u8> {
    [WORKSPACE_PREFIX, id.to_string().as_bytes()].concat()
}
fn layer_key(id: LayerId) -> Vec<u8> {
    [LAYER_PREFIX, id.to_string().as_bytes()].concat()
}
fn lease_key(workspace: WorkspaceId, id: LeaseId) -> Vec<u8> {
    format!("lease/{workspace}/{id}").into_bytes()
}
fn lease_prefix(workspace: WorkspaceId) -> Vec<u8> {
    format!("lease/{workspace}/").into_bytes()
}
fn lease_index_key(id: LeaseId) -> Vec<u8> {
    [LEASE_INDEX_PREFIX, id.to_string().as_bytes()].concat()
}
fn journal_key(workspace: WorkspaceId, id: JournalId) -> Vec<u8> {
    format!("journal/{workspace}/{id}").into_bytes()
}
fn journal_prefix(workspace: WorkspaceId) -> Vec<u8> {
    format!("journal/{workspace}/").into_bytes()
}
fn journal_index_key(id: JournalId) -> Vec<u8> {
    [JOURNAL_INDEX_PREFIX, id.to_string().as_bytes()].concat()
}
fn snapshot_key(id: SnapshotId) -> Vec<u8> {
    [SNAPSHOT_PREFIX, id.to_string().as_bytes()].concat()
}
fn snapshot_name_key(name: &str) -> Vec<u8> {
    [SNAPSHOT_NAME_PREFIX, hex::encode(name).as_bytes()].concat()
}
fn allocator_key(name: &str) -> Vec<u8> {
    [ALLOCATOR_PREFIX, name.as_bytes()].concat()
}

fn allocator_name_from_key(key: &[u8]) -> Result<String, WorkspaceError> {
    let name = key.strip_prefix(LEGACY_ALLOCATOR_PREFIX).ok_or_else(|| {
        WorkspaceError::CorruptMetadata("invalid legacy allocator key prefix".into())
    })?;
    String::from_utf8(name.to_vec())
        .map_err(|_| WorkspaceError::CorruptMetadata("invalid legacy allocator key".into()))
}

fn put_control(value: &ControlHeader) -> Result<KvWrite, WorkspaceError> {
    let payload = bincode::serialize(value)
        .map_err(|error| WorkspaceError::Backend(format!("encode control: {error}")))?;
    Ok(KvWrite::Put {
        key: CONTROL_KEY.to_vec(),
        value: [CONTROL_MAGIC.as_slice(), payload.as_slice()].concat(),
    })
}

fn decode_control(raw: &[u8]) -> Result<ControlHeader, WorkspaceError> {
    let payload = raw
        .strip_prefix(CONTROL_MAGIC)
        .ok_or_else(|| WorkspaceError::CorruptMetadata("invalid control marker".into()))?;
    bincode::deserialize(payload)
        .map_err(|error| WorkspaceError::CorruptMetadata(format!("decode control: {error}")))
}

fn encode_migration(state: &MigrationState) -> Result<Vec<u8>, WorkspaceError> {
    let payload = bincode::serialize(state)
        .map_err(|error| WorkspaceError::Backend(format!("encode migration: {error}")))?;
    Ok([b"BWSMG002".as_slice(), payload.as_slice()].concat())
}

fn decode_migration(raw: &[u8]) -> Result<MigrationState, WorkspaceError> {
    let payload = raw
        .strip_prefix(b"BWSMG002")
        .ok_or_else(|| WorkspaceError::CorruptMetadata("invalid migration marker".into()))?;
    bincode::deserialize(payload)
        .map_err(|error| WorkspaceError::CorruptMetadata(format!("decode migration: {error}")))
}

async fn retry_backoff(attempt: usize) {
    let base_ms = (1_u64 << attempt.min(7)).min(100);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .subsec_nanos();
    let jitter_ms = u64::from(nanos) % (base_ms + 1);
    sleep(Duration::from_millis((base_ms + jitter_ms).min(100))).await;
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

fn catalog_roots_present(state: &CatalogState) -> bool {
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
            .filter(|lease| matches!(lease.state, LeaseState::Active | LeaseState::Releasing))
            .map(|lease| lease.base_revision.layer_id),
    );
    pending.extend(
        state
            .journals
            .values()
            .filter(|journal| !matches!(journal.phase, SealPhase::Completed | SealPhase::Aborted))
            .map(|journal| journal.old_head_layer_id),
    );
    let mut seen = HashSet::new();
    while let Some(layer_id) = pending.pop() {
        if !seen.insert(layer_id) {
            continue;
        }
        let Some(layer) = state.layers.get(&layer_id) else {
            return false;
        };
        pending.extend(layer.parent_layer_id);
    }
    true
}

fn reachable_layers(state: &CatalogState, lease_cutoff: i64) -> HashSet<LayerId> {
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
            .filter(|lease| match lease.state {
                LeaseState::Active | LeaseState::Releasing | LeaseState::Expired => {
                    lease.expires_at_ns > lease_cutoff
                }
                LeaseState::Released => lease.updated_at_ns > lease_cutoff,
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
    use crate::workspace_overlay::ids::{JournalId, LeaseId, SnapshotId};
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
    async fn missing_snapshot_is_a_typed_point_lookup_without_scan() {
        let (store, _workspace, _lease, _guard) = initialized().await;
        let scans_before = store.backend.scans.load(Ordering::Relaxed);
        let snapshot_id = SnapshotId::from_uuid(id(99));

        let error = store.load_snapshot(snapshot_id).await.unwrap_err();

        assert!(matches!(
            error,
            WorkspaceError::SnapshotNotFound(id) if id == snapshot_id
        ));
        assert_eq!(store.backend.scans.load(Ordering::Relaxed), scans_before);
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
                && keys.iter().any(|key| key.starts_with(WORKSPACE_PREFIX))
                && keys.iter().any(|key| key.starts_with(LAYER_PREFIX))
                && keys.iter().any(|key| key.starts_with(LEASE_PREFIX))
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
    async fn legacy_catalog_migrates_hot_values_and_resumes_after_staging() {
        let source = KvWorkspaceStore::new(MemoryBackend::default());
        source.initialize_workspace_schema().await.unwrap();
        let workspace = source
            .create_volume_root(create_request(2000))
            .await
            .unwrap();
        let root = source
            .load_layer(workspace.fork_base.as_ref().unwrap().layer_id)
            .await
            .unwrap();
        let lease = source
            .acquire_lease(AcquireLease {
                workspace_id: workspace.workspace_id,
                lease_id: LeaseId::from_uuid(id(2005)),
                holder_generation: 5,
                ttl_ns: 120_000_000_000,
            })
            .await
            .unwrap();
        let guard = HeadGuard {
            workspace_id: workspace.workspace_id,
            expected_head_layer_id: workspace.head_layer_id,
            expected_head_epoch: 0,
            lease_id: lease.lease_id,
            holder_generation: 5,
        };
        source
            .apply_namespace_mutation(NamespaceMutation {
                guard: guard.clone(),
                dentries: vec![DentryDelta::put(
                    workspace.head_layer_id,
                    1,
                    b"migrated".to_vec(),
                    2,
                    1,
                    0,
                )],
                inodes: Vec::new(),
            })
            .await
            .unwrap();
        let journal = source
            .begin_seal(BeginSeal {
                guard,
                journal_id: JournalId::from_uuid(id(2006)),
                new_head_layer_id: LayerId::from_uuid(id(2007)),
            })
            .await
            .unwrap();
        source
            .abort_recoverable_seal(AbortSeal {
                journal_id: journal.journal_id,
                reason: "interrupted".into(),
            })
            .await
            .unwrap();
        source
            .release_lease(ReleaseLease {
                lease_id: lease.lease_id,
                holder_generation: 5,
            })
            .await
            .unwrap();
        let snapshot = source
            .create_snapshot(CreateSnapshot {
                snapshot_id: SnapshotId::from_uuid(id(2008)),
                name: Some("before-migration".into()),
                revision: workspace.fork_base.clone().unwrap(),
                owner_id: None,
            })
            .await
            .unwrap();
        let final_workspace = source.load_workspace(workspace.workspace_id).await.unwrap();
        let head = source.load_layer(workspace.head_layer_id).await.unwrap();
        let final_lease = source
            .list_leases(workspace.workspace_id)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let final_journal = source.load_seal_journal(journal.journal_id).await.unwrap();
        let mut stale_head = head.clone();
        stale_head.next_sequence = 1;
        let legacy_workspace = LegacyWorkspaceRecord {
            workspace_id: final_workspace.workspace_id,
            head_layer_id: final_workspace.head_layer_id,
            head_epoch: final_workspace.head_epoch,
            fork_base: final_workspace.fork_base.clone(),
            owner_id: final_workspace.owner_id.clone(),
            state: final_workspace.state,
            created_at_ns: final_workspace.created_at_ns,
            updated_at_ns: final_workspace.updated_at_ns,
        };
        let legacy = LegacyControlState {
            schema_version: WORKSPACE_SCHEMA_VERSION,
            header: source.load_volume_header().await.unwrap(),
            workspaces: BTreeMap::from([(workspace.workspace_id, legacy_workspace.clone())]),
            layers: BTreeMap::from([(root.layer_id, root.clone()), (head.layer_id, stale_head)]),
            snapshots: BTreeMap::from([(snapshot.snapshot_id, snapshot.clone())]),
            leases: BTreeMap::from([(final_lease.lease_id, final_lease.clone())]),
            journals: BTreeMap::from([(final_journal.journal_id, final_journal.clone())]),
            allocators: BTreeMap::from([
                ("inode".into(), 2),
                ("slice".into(), 1),
                ("sealed_version".into(), 2),
            ]),
        };
        let backend = MemoryBackend::default();
        {
            let mut records = backend.records.lock().await;
            records.insert(CONTROL_KEY.to_vec(), encode(&legacy).unwrap());
            records.insert(
                format!("hot/workspace/{}", workspace.workspace_id).into_bytes(),
                encode(&legacy_workspace).unwrap(),
            );
            records.insert(
                format!("hot/layer/{}", head.layer_id).into_bytes(),
                encode(&head).unwrap(),
            );
            records.insert(
                format!("hot/lease/{}", lease.lease_id).into_bytes(),
                encode(&final_lease).unwrap(),
            );
            records.insert(
                format!("hot/snapshot/{}", snapshot.snapshot_id).into_bytes(),
                encode(&snapshot).unwrap(),
            );
        }
        for entry in source
            .scan_entries(dentry_layer_prefix(head.layer_id))
            .await
            .unwrap()
        {
            backend.records.lock().await.insert(entry.key, entry.value);
        }
        let store = KvWorkspaceStore::new(backend.clone());
        assert!(
            store
                .load_volume_header()
                .await
                .unwrap_err()
                .to_string()
                .contains("brewfs workspace migrate")
        );
        let (migration, checks) = store
            .legacy_state(&backend.get(CONTROL_KEY).await.unwrap().unwrap())
            .await
            .unwrap();
        let marker = encode_migration(&migration).unwrap();
        let mut writes = checks
            .iter()
            .skip(1)
            .map(|check| KvWrite::Delete {
                key: check.key.clone(),
            })
            .collect::<Vec<_>>();
        writes.push(KvWrite::Put {
            key: CONTROL_KEY.to_vec(),
            value: marker,
        });
        assert!(backend.compare_and_swap(&checks, &writes).await.unwrap());
        store.stage_migration(&migration).await.unwrap();
        assert!(
            store
                .load_volume_header()
                .await
                .unwrap_err()
                .to_string()
                .contains("brewfs workspace migrate")
        );
        store.initialize_workspace_schema().await.unwrap();
        store.initialize_workspace_schema().await.unwrap();
        assert_eq!(
            store.load_workspace(workspace.workspace_id).await.unwrap(),
            final_workspace
        );
        assert_eq!(store.load_layer(head.layer_id).await.unwrap(), head);
        assert_eq!(
            store.load_snapshot(snapshot.snapshot_id).await.unwrap(),
            snapshot
        );
        assert_eq!(
            store.list_leases(workspace.workspace_id).await.unwrap(),
            vec![final_lease]
        );
        assert_eq!(
            store.load_seal_journal(journal.journal_id).await.unwrap(),
            final_journal
        );
        assert_eq!(
            store
                .get_dentry_deltas(DentryQuery {
                    layer_ids: vec![head.layer_id],
                    parent_ino: 1,
                    name: Some(b"migrated".to_vec()),
                })
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(store.allocate_id("inode").await.unwrap(), 2);
        assert!(
            backend
                .get(format!("hot/layer/{}", head.layer_id).as_bytes())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn gc_rejects_a_root_whose_layer_chain_is_missing() {
        let backend = MemoryBackend::default();
        let store = KvWorkspaceStore::new(backend.clone());
        store.initialize_workspace_schema().await.unwrap();
        let workspace = store
            .create_volume_root(create_request(2900))
            .await
            .unwrap();
        let base = workspace.fork_base.as_ref().unwrap().layer_id;
        let key = layer_key(base);
        let current = backend.get(&key).await.unwrap();
        assert!(
            backend
                .compare_and_swap(
                    &[KvCheck {
                        key: key.clone(),
                        expected: current
                    }],
                    &[KvWrite::Delete { key }],
                )
                .await
                .unwrap()
        );
        assert!(matches!(
            store.gc_snapshot(i64::MAX, 0).await,
            Err(WorkspaceError::Busy)
        ));
    }

    #[tokio::test]
    async fn gc_marks_only_unreferenced_layers_and_fences_later_roots() {
        let store = KvWorkspaceStore::new(MemoryBackend::default());
        store.initialize_workspace_schema().await.unwrap();
        let root_workspace = store
            .create_volume_root(create_request(3000))
            .await
            .unwrap();
        let mut orphan = store
            .load_layer(root_workspace.fork_base.as_ref().unwrap().layer_id)
            .await
            .unwrap();
        orphan.layer_id = LayerId::from_uuid(id(3010));
        let revision = revision_from_layer(&orphan).unwrap();
        let mut txn = store.topology_txn();
        assert!(txn.read_layer(orphan.layer_id).await.unwrap().is_none());
        txn.put_layer(&orphan).unwrap();
        assert!(txn.commit().await.unwrap());
        let snapshot = store
            .create_snapshot(CreateSnapshot {
                snapshot_id: SnapshotId::from_uuid(id(3011)),
                name: None,
                revision: revision.clone(),
                owner_id: None,
            })
            .await
            .unwrap();
        let request = DeleteLayerMetadata {
            layer_ids: vec![orphan.layer_id],
            now_ns: i64::MAX,
            lease_grace_ns: 0,
        };
        assert!(matches!(
            store.delete_layer_metadata(request.clone()).await,
            Err(WorkspaceError::Busy)
        ));
        store.delete_snapshot(snapshot.snapshot_id).await.unwrap();
        store.delete_layer_metadata(request).await.unwrap();
        assert_eq!(
            store.load_layer(orphan.layer_id).await.unwrap().state,
            LayerState::Deleting
        );
        assert!(
            store
                .create_snapshot(CreateSnapshot {
                    snapshot_id: SnapshotId::from_uuid(id(3012)),
                    name: None,
                    revision: revision.clone(),
                    owner_id: None,
                })
                .await
                .is_err()
        );
        assert!(
            store
                .create_workspace(CreateWorkspace {
                    workspace_id: WorkspaceId::from_uuid(id(3013)),
                    head_layer_id: LayerId::from_uuid(id(3014)),
                    base_revision: revision,
                    owner_id: None,
                })
                .await
                .is_err()
        );
        store
            .finalize_layer_metadata_deletion(vec![orphan.layer_id])
            .await
            .unwrap();
        assert!(matches!(
            store.load_layer(orphan.layer_id).await,
            Err(WorkspaceError::LayerNotFound(_))
        ));
    }

    #[tokio::test]
    async fn concurrent_root_creation_and_gc_mark_preserve_each_committed_root() {
        let store = Arc::new(KvWorkspaceStore::new(MemoryBackend::default()));
        store.initialize_workspace_schema().await.unwrap();
        let workspace = store
            .create_volume_root(create_request(3200))
            .await
            .unwrap();
        let mut orphan = store
            .load_layer(workspace.fork_base.as_ref().unwrap().layer_id)
            .await
            .unwrap();
        orphan.layer_id = LayerId::from_uuid(id(3210));
        let revision = revision_from_layer(&orphan).unwrap();
        let mut txn = store.topology_txn();
        txn.read_layer(orphan.layer_id).await.unwrap();
        txn.put_layer(&orphan).unwrap();
        assert!(txn.commit().await.unwrap());
        let barrier = Arc::new(Barrier::new(2));
        let fork_store = Arc::clone(&store);
        let fork_barrier = Arc::clone(&barrier);
        let fork = tokio::spawn(async move {
            fork_barrier.wait().await;
            fork_store
                .create_workspace(CreateWorkspace {
                    workspace_id: WorkspaceId::from_uuid(id(3211)),
                    head_layer_id: LayerId::from_uuid(id(3212)),
                    base_revision: revision,
                    owner_id: None,
                })
                .await
        });
        barrier.wait().await;
        let deletion = store
            .delete_layer_metadata(DeleteLayerMetadata {
                layer_ids: vec![orphan.layer_id],
                now_ns: i64::MAX,
                lease_grace_ns: 0,
            })
            .await;
        let fork = fork.await.unwrap();
        match (fork, deletion) {
            (Ok(fork), Err(WorkspaceError::Busy)) => {
                assert_eq!(
                    store.load_layer(orphan.layer_id).await.unwrap().state,
                    LayerState::Sealed
                );
                assert_eq!(store.load_workspace(fork.workspace_id).await.unwrap(), fork);
            }
            (Err(WorkspaceError::LayerNotFound(_)), Ok(())) => {
                assert_eq!(
                    store.load_layer(orphan.layer_id).await.unwrap().state,
                    LayerState::Deleting
                );
            }
            (other, result) => panic!("unexpected fork/GC race outcome: {other:?} / {result:?}"),
        }
    }

    #[tokio::test]
    async fn topology_transaction_rejects_writes_without_prior_reads() {
        let backend = MemoryBackend::default();
        let mut transaction = TopologyTxn::new(&backend);
        let row = WorkspaceRecord {
            workspace_id: WorkspaceId::from_uuid(id(1200)),
            head_layer_id: LayerId::from_uuid(id(1201)),
            head_epoch: 0,
            fork_base: None,
            owner_id: None,
            state: WorkspaceState::Active,
            active_lease: None,
            created_at_ns: 0,
            updated_at_ns: 0,
        };
        assert!(matches!(
            transaction.put_workspace(&row),
            Err(WorkspaceError::CorruptMetadata(_))
        ));
        assert!(backend.cas_checks.lock().await.is_empty());
    }

    #[tokio::test]
    async fn topology_transaction_repeated_reads_keep_the_original_condition() {
        let backend = MemoryBackend::default();
        let store = KvWorkspaceStore::new(backend.clone());
        store.initialize_workspace_schema().await.unwrap();
        let workspace = store
            .create_volume_root(create_request(1500))
            .await
            .unwrap();
        let mut transaction = TopologyTxn::new(&backend);
        assert_eq!(
            transaction
                .read_workspace(workspace.workspace_id)
                .await
                .unwrap(),
            Some(workspace.clone())
        );
        store
            .create_workspace(CreateWorkspace {
                workspace_id: WorkspaceId::from_uuid(id(1600)),
                head_layer_id: LayerId::from_uuid(id(1601)),
                base_revision: workspace.fork_base.clone().unwrap(),
                owner_id: None,
            })
            .await
            .unwrap();
        let key = workspace_key(workspace.workspace_id);
        let (raw, _) = store
            .load_hot::<WorkspaceRecord>(key.clone())
            .await
            .unwrap();
        let mut updated = workspace.clone();
        updated.updated_at_ns += 1;
        assert!(
            backend
                .compare_and_swap(
                    &[KvCheck {
                        key: key.clone(),
                        expected: raw
                    }],
                    &[put(key, &updated).unwrap()]
                )
                .await
                .unwrap()
        );
        assert_eq!(
            transaction
                .read_workspace(workspace.workspace_id)
                .await
                .unwrap(),
            Some(workspace.clone())
        );
        transaction.put_workspace(&workspace).unwrap();
        assert!(!transaction.commit().await.unwrap());
        assert_eq!(
            store.load_workspace(workspace.workspace_id).await.unwrap(),
            updated
        );
    }

    #[tokio::test]
    async fn topology_transaction_rejects_changes_to_a_read_entity_atomically() {
        let backend = MemoryBackend::default();
        let store = KvWorkspaceStore::new(backend.clone());
        store.initialize_workspace_schema().await.unwrap();
        let root = store
            .create_volume_root(create_request(1300))
            .await
            .unwrap();
        let base = store.load_layer_chain(root.head_layer_id).await.unwrap();
        let revision = BaseRevision {
            layer_id: base[1].layer_id,
            sealed_version: base[1].sealed_version.unwrap(),
            root_hash: base[1].root_hash.unwrap(),
        };
        let workspace_id = WorkspaceId::from_uuid(id(1400));
        let head_id = LayerId::from_uuid(id(1401));
        let mut transaction = TopologyTxn::new(&backend);
        transaction.read_layer(revision.layer_id).await.unwrap();
        transaction.read_workspace(workspace_id).await.unwrap();
        transaction.read_layer(head_id).await.unwrap();
        store
            .create_workspace(CreateWorkspace {
                workspace_id,
                head_layer_id: head_id,
                base_revision: revision,
                owner_id: None,
            })
            .await
            .unwrap();
        let replacement = store.load_workspace(workspace_id).await.unwrap();
        transaction.put_workspace(&replacement).unwrap();
        assert!(!transaction.commit().await.unwrap());
        assert_eq!(
            store.load_workspace(workspace_id).await.unwrap(),
            replacement
        );
    }

    #[tokio::test]
    async fn topology_transaction_checks_only_declared_entities() {
        let backend = MemoryBackend::default();
        let store = KvWorkspaceStore::new(backend.clone());
        store.initialize_workspace_schema().await.unwrap();
        let root = store.create_volume_root(create_request(300)).await.unwrap();
        let base = store.load_layer_chain(root.head_layer_id).await.unwrap();
        let revision = BaseRevision {
            layer_id: base[1].layer_id,
            sealed_version: base[1].sealed_version.unwrap(),
            root_hash: base[1].root_hash.unwrap(),
        };
        backend.cas_checks.lock().await.clear();
        for index in 0..64 {
            store
                .create_workspace(CreateWorkspace {
                    workspace_id: WorkspaceId::from_uuid(id(400 + index)),
                    head_layer_id: LayerId::from_uuid(id(500 + index)),
                    base_revision: revision.clone(),
                    owner_id: None,
                })
                .await
                .unwrap();
        }
        assert_eq!(store.list_workspaces().await.unwrap().len(), 65);
        let checks = backend.cas_checks.lock().await;
        assert_eq!(checks.len(), 64);
        assert!(checks.iter().all(|keys| {
            keys.len() == 4
                && keys
                    .iter()
                    .filter(|key| key.as_slice() == CONTROL_KEY)
                    .count()
                    == 1
        }));
    }

    #[tokio::test]
    async fn concurrent_forks_from_one_revision_have_distinct_heads() {
        let store = Arc::new(KvWorkspaceStore::new(MemoryBackend::default()));
        store.initialize_workspace_schema().await.unwrap();
        let root = store.create_volume_root(create_request(700)).await.unwrap();
        let base = store.load_layer_chain(root.head_layer_id).await.unwrap();
        let revision = BaseRevision {
            layer_id: base[1].layer_id,
            sealed_version: base[1].sealed_version.unwrap(),
            root_hash: base[1].root_hash.unwrap(),
        };
        let barrier = Arc::new(Barrier::new(64));
        let mut forks = Vec::new();
        for index in 0..64 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let revision = revision.clone();
            forks.push(tokio::spawn(async move {
                barrier.wait().await;
                let workspace_id = WorkspaceId::from_uuid(id(800 + index));
                let head_layer_id = LayerId::from_uuid(id(900 + index));
                let workspace = store
                    .create_workspace(CreateWorkspace {
                        workspace_id,
                        head_layer_id,
                        base_revision: revision,
                        owner_id: None,
                    })
                    .await?;
                assert_eq!(store.load_workspace(workspace_id).await?, workspace);
                assert_eq!(
                    store.load_layer(head_layer_id).await?.owner_workspace_id,
                    Some(workspace_id)
                );
                Ok::<(), WorkspaceError>(())
            }));
        }
        for fork in forks {
            fork.await.unwrap().unwrap();
        }
        assert_eq!(store.list_workspaces().await.unwrap().len(), 65);
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
        assert!(
            checks
                .iter()
                .all(|keys| { keys.len() == 1 && keys[0].as_slice() != CONTROL_KEY })
        );
        assert!(checks[0][0].starts_with(LEASE_PREFIX));
        assert!(
            checks[1..]
                .iter()
                .all(|keys| keys[0].starts_with(ALLOCATOR_PREFIX))
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
