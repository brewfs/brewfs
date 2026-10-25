//! Backend-neutral workspace catalog stored in Redis or TiKV.
//!
//! Lifecycle transitions use exact entity-key CAS. CONTROL contains only the
//! catalog header; ControlState is an ephemeral scoped/census view. Packed-v3
//! derived authorities join the same prepared packet before its actual CAS.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::Mutex;

use super::kv_backend::{KvCheck, KvEntry, KvReadLimits, KvWrite, WorkspaceKvBackend};
use crate::workspace_overlay::catalog::*;
#[cfg(test)]
use crate::workspace_overlay::catalog::{InstallTestBase, TestBaseResult};
use crate::workspace_overlay::digest::{CanonicalLayerDelta, delta_digest, root_hash};
use crate::workspace_overlay::error::{ConflictDetail, WorkspaceError};
use crate::workspace_overlay::ids::{JournalId, LayerId, LeaseId, SnapshotId, WorkspaceId};
use crate::workspace_overlay::model::*;
use crate::workspace_overlay::publish::binding::{
    InstallPackedLowerBinding, PackedLowerBindingRecord, PublishPackedLowerBinding,
};
use crate::workspace_overlay::resolver::validate_layer_chain;

mod topology_scope;
use topology_scope::*;

mod extent_compaction;
mod native_finalization;
mod native_read_conflict;
mod native_reverse;
mod native_slice_deletion;
pub mod packed_admin;
mod packed_carrier_basis;
mod packed_cli_mount;
mod packed_enumeration;
pub(crate) mod packed_journal;
mod packed_mount_writeback;
pub mod packed_native_freeze;
mod packed_paths;
mod packed_permissions;
pub mod packed_reader_pins;
mod packed_writer_authority;
mod packed_writer_fences;

#[cfg(test)]
mod topology_test_support;
#[cfg(test)]
use topology_test_support::*;

const CONTROL_KEY: &[u8] = b"control";
const HOT_WORKSPACE_PREFIX: &[u8] = b"ws/";
const HOT_LAYER_PREFIX: &[u8] = b"layer/";
const HOT_LEASE_PREFIX: &[u8] = b"lease/";
const HOT_SNAPSHOT_PREFIX: &[u8] = b"snapshot/";
const HOT_ALLOCATOR_PREFIX: &[u8] = b"alloc/";
const ENVELOPE_MAGIC: &[u8; 8] = b"BWSKV001";

const HOT_JOURNAL_PREFIX: &[u8] = b"journal/";
const WORKSPACE_PREFIX: &[u8] = HOT_WORKSPACE_PREFIX;
const LAYER_PREFIX: &[u8] = HOT_LAYER_PREFIX;
const LEASE_PREFIX: &[u8] = HOT_LEASE_PREFIX;
const JOURNAL_PREFIX: &[u8] = HOT_JOURNAL_PREFIX;
const SNAPSHOT_PREFIX: &[u8] = HOT_SNAPSHOT_PREFIX;
const ALLOCATOR_PREFIX: &[u8] = HOT_ALLOCATOR_PREFIX;
const LEASE_INDEX_PREFIX: &[u8] = b"lease-id/";
const JOURNAL_INDEX_PREFIX: &[u8] = b"journal-id/";
const SNAPSHOT_NAME_PREFIX: &[u8] = b"snapshot-name/";
const LEGACY_WORKSPACE_PREFIX: &[u8] = b"hot/workspace/";
const LEGACY_LAYER_PREFIX: &[u8] = b"hot/layer/";
const LEGACY_LEASE_PREFIX: &[u8] = b"hot/lease/";
const LEGACY_SNAPSHOT_PREFIX: &[u8] = b"hot/snapshot/";
const LEGACY_ALLOCATOR_PREFIX: &[u8] = b"hot/allocator/";
const CONTROL_MAGIC: &[u8; 8] = b"BWSCT002";
const CATALOG_FORMAT: u32 = 2;
const TOPOLOGY_GENERATION_KEY: &[u8] = b"packed/v3/topology-generation";

// TiKV can hold a bounded transaction lock across several scheduler ticks
// while concurrent mounts are flushing writes. Keep this finite, but give
// remote backends enough attempts to observe the next generation rather than
// surfacing a transient Busy to the caller.
const CAS_MAX_RETRIES: usize = 256;
const VOLUME_FORMAT: &str = "workspace-v1";
const PACKED_CLAIM: &[u8] = b"PWC3";
const PACKED_ROOT_GENERATION_KEY: &[u8] = b"packed/v3/root-generation";
const LAYER_INVENTORY_GENERATION_KEY: &[u8] = b"packed/v3/layer-inventory-generation";
// PWB3 root enumeration is an admin/GC proof, so never materialize an
// unbounded packed history/current namespace from Redis or TiKV. The row cap
// matches the native GC census tier; the byte cap fails closed before retained
// checks can exceed the canonical metadata budget.
const PACKED_BINDING_SCAN_PAGE_RECORDS: usize = 32;
const PACKED_BINDING_SCAN_MAX_ROWS: usize = 4096;
const PACKED_BINDING_SCAN_MAX_PAGES: usize =
    PACKED_BINDING_SCAN_MAX_ROWS / PACKED_BINDING_SCAN_PAGE_RECORDS + 1;
const PACKED_BINDING_SCAN_MAX_BYTES: usize = 16 << 20;
const PACKED_BINDING_SCAN_ADMISSION_BYTES: u64 = 16 << 20;
const OPEN_V3_PREFIX: &[u8] = b"open/v3/";
const OPEN_RECOVERY_PREFIX: &[u8] = b"open/v3/recovery/";
const VOLUME_HEADER_KEY: &[u8] = b"volume/header";
const OPEN_OWNER_MAX_BYTES: usize = 256;
const OPEN_RECORD_MAX_BYTES: usize = 4096;
const OPEN_CONTROL_MAX_BYTES: usize = 4 << 20;
const OPEN_RECOVERY_MAX_BYTES: usize = 256;

/// Hard admission limits for every v3 open read. Open metadata is composed
/// of a bounded set of small records (the largest PWB3 binding is 8 KiB), so
/// the remote backend must reject an oversized value before materializing it.
/// The response limit covers the complete multi-key packet, rather than just
/// one value, because Redis Lua and TiKV enforce it at the transport boundary.
const OPEN_V3_MAX_RECORDS: usize = 32;
const OPEN_V3_MAX_KEY_BYTES: usize = 1024;
const OPEN_V3_MAX_VALUE_BYTES: usize = 12 << 10;
const OPEN_V3_MAX_TOTAL_BYTES: usize = 256 << 10;
const OPEN_V3_MAX_RESPONSE_BYTES: usize = 128 << 10;
const OPEN_V3_MAX_DATA_REQUESTS: usize = 32;

// Native delta reads are part of the Redis/TiKV metadata path. Keep each
// backend response small and retain a finite aggregate admission while still
// allowing a normal inode's complete xattr/ACL/extent history to span pages.
const DELTA_SCAN_PAGE_RECORDS: usize = 32;
const DELTA_SCAN_MAX_ROWS: usize = 4096;
const DELTA_SCAN_MAX_BYTES: usize = 32 << 20;

// Administrative/listing paths still return a complete logical result, but
// they must never materialize an unbounded Redis/TiKV prefix in one request.
// Keep each backend response small and fail closed when a caller would exceed
// the retained metadata budget.
const GENERIC_SCAN_PAGE_RECORDS: usize = 64;
const GENERIC_SCAN_MAX_ROWS: usize = 16_384;
const GENERIC_SCAN_MAX_BYTES: usize = 64 << 20;

fn generic_scan_limits() -> KvReadLimits {
    KvReadLimits {
        max_records: GENERIC_SCAN_PAGE_RECORDS,
        // All backends share the 1024-byte logical key schema for bounded
        // keyset pages. Larger values are rejected before reaching a
        // backend, so keep generic scans within that contract.
        max_key_bytes: 1024,
        max_value_bytes: 256 << 10,
        max_total_bytes: 4 << 20,
        max_response_bytes: 4 << 20,
        max_data_requests: 64,
    }
}

fn delta_scan_limits() -> KvReadLimits {
    KvReadLimits {
        max_records: DELTA_SCAN_PAGE_RECORDS,
        max_key_bytes: 1024,
        max_value_bytes: 48 << 10,
        max_total_bytes: 2 << 20,
        max_response_bytes: 2 << 20,
        max_data_requests: 32,
    }
}

fn validate_delta_scan_page(
    page: &[KvEntry],
    prefix: &[u8],
    after: Option<&[u8]>,
) -> Result<usize, WorkspaceError> {
    if page.len() > DELTA_SCAN_PAGE_RECORDS {
        return Err(WorkspaceError::Fenced);
    }
    let mut bytes = 0usize;
    let mut previous = after;
    for entry in page {
        if !entry.key.starts_with(prefix) || previous.is_some_and(|key| entry.key.as_slice() <= key)
        {
            return Err(WorkspaceError::Fenced);
        }
        bytes = bytes
            .checked_add(entry.key.len())
            .and_then(|sum| sum.checked_add(entry.value.len()))
            .ok_or(WorkspaceError::Fenced)?;
        previous = Some(&entry.key);
    }
    Ok(bytes)
}

fn validate_generic_scan_page(
    page: &[KvEntry],
    prefix: &[u8],
    after: Option<&[u8]>,
) -> Result<usize, WorkspaceError> {
    let limits = generic_scan_limits();
    if page.len() > limits.max_records {
        return Err(WorkspaceError::Fenced);
    }
    let mut bytes = 0usize;
    let mut previous = after;
    for entry in page {
        if !entry.key.starts_with(prefix) || previous.is_some_and(|key| entry.key.as_slice() <= key)
        {
            return Err(WorkspaceError::Fenced);
        }
        if entry.key.len() > limits.max_key_bytes || entry.value.len() > limits.max_value_bytes {
            return Err(WorkspaceError::InvalidReadPlan(
                "generic metadata page entry exceeds bounded value limits".into(),
            ));
        }
        bytes = bytes
            .checked_add(entry.key.len())
            .and_then(|sum| sum.checked_add(entry.value.len()))
            .ok_or(WorkspaceError::Fenced)?;
        if bytes > limits.max_total_bytes {
            return Err(WorkspaceError::InvalidReadPlan(
                "generic metadata page exceeds bounded byte limits".into(),
            ));
        }
        previous = Some(&entry.key);
    }
    Ok(bytes)
}

fn v3_open_read_limits() -> KvReadLimits {
    KvReadLimits {
        max_records: OPEN_V3_MAX_RECORDS,
        max_key_bytes: OPEN_V3_MAX_KEY_BYTES,
        max_value_bytes: OPEN_V3_MAX_VALUE_BYTES,
        max_total_bytes: OPEN_V3_MAX_TOTAL_BYTES,
        max_response_bytes: OPEN_V3_MAX_RESPONSE_BYTES,
        max_data_requests: OPEN_V3_MAX_DATA_REQUESTS,
    }
}

/// State persisted by the v3 open sidecar. This only fences sidecar calls;
/// mount, mutation and seal recovery do not yet consume the open token.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum V3OpenState {
    Recovering = 0,
    Ready = 1,
}

/// Fencing token returned by KvWorkspaceStore::open_workspace_v3.
/// This token currently governs only the open/ready/renew/close sidecar APIs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3OpenToken {
    pub workspace_id: WorkspaceId,
    pub owner_id: String,
    pub generation: u64,
    pub expires_at_ns: i64,
    pub state: V3OpenState,
    pub recovery_required: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct V3OpenRecord {
    workspace_id: WorkspaceId,
    owner_id: String,
    generation: u64,
    expires_at_ns: i64,
    state: V3OpenState,
    recovery_required: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct V3RecoveryRecord {
    workspace_id: WorkspaceId,
    incomplete: bool,
}

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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ControlHeader {
    schema_version: u32,
    header: Option<VolumeHeader>,
    catalog_format: u32,
}

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

struct V3OpenContext {
    state: ControlState,
    current: Option<V3OpenRecord>,
    packed_binding: Option<PackedLowerBindingRecord>,
    recovery_required: bool,
    checks: Vec<KvCheck>,
    now: i64,
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
    runtime_only: bool,
    packed_reader_pin_budget:
        std::sync::OnceLock<Arc<crate::workspace_overlay::packed_v3::wire005::V3MountBudget>>,
    native_auxiliary_budget:
        std::sync::OnceLock<Arc<crate::workspace_overlay::packed_v3::wire005::V3MountBudget>>,
    slice_ids: IdBlock,
    inode_ids: IdBlock,
    /// Opt-in: a mounted runtime store reserves inode IDs in blocks. Admin,
    /// publication and test stores keep dense one-at-a-time allocation.
    inode_blocks: bool,
    /// Last head record this store committed through a packed versioned
    /// mutation. A hint only: callers use it as an expected version and the
    /// CAS fails with Busy if anything changed since.
    committed_head: Arc<std::sync::Mutex<Option<LayerRecord>>>,
    /// Post-commit image of the packed writer's view keys; see
    /// `packed_permissions::CommitViewCache`. Shared with mutation drivers.
    commit_view: Arc<std::sync::Mutex<Option<packed_permissions::CommitViewCache>>>,
}

tokio::task_local! {
    /// Values a commit already read in its own consistent view read, keyed
    /// by exact key. Derived-CAS preparation consults it instead of issuing a
    /// second point read. Each value is still a CAS check, so staleness can
    /// only fail the commit (Busy -> bounded retry), never apply wrong state.
    static PREFETCHED_POINTS: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>;
}

/// All requested keys from the commit's prefetch, or None to read normally.
pub(super) fn prefetched_points(keys: &[Vec<u8>]) -> Option<Vec<Option<Vec<u8>>>> {
    PREFETCHED_POINTS
        .try_with(|points| {
            keys.iter()
                .map(|key| points.get(key).cloned())
                .collect::<Option<Vec<_>>>()
        })
        .ok()
        .flatten()
}

pub(super) async fn with_prefetched_points<F: std::future::Future>(
    points: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    future: F,
) -> F::Output {
    PREFETCHED_POINTS.scope(points, future).await
}

/// Per-store reserved range of slice IDs, `[next, end)`.
#[derive(Default)]
struct IdBlock(std::sync::Mutex<(i64, i64)>);

impl IdBlock {
    fn take(&self) -> Option<i64> {
        let mut range = self.0.lock().unwrap();
        (range.0 < range.1).then(|| {
            range.0 += 1;
            range.0 - 1
        })
    }
    fn refill(&self, first: i64, count: i64) {
        let mut range = self.0.lock().unwrap();
        if range.0 >= range.1 {
            *range = (first, first + count);
        }
    }
}

const SLICE_ID_BLOCK: i64 = 64;
const INODE_ID_BLOCK: i64 = 64;

struct TopologyTxn<'a, B> {
    store: &'a KvWorkspaceStore<B>,
    checks: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    writes: BTreeMap<Vec<u8>, KvWrite>,
    deadline: Option<i64>,
}

impl<'a, B: WorkspaceKvBackend> TopologyTxn<'a, B> {
    fn new(store: &'a KvWorkspaceStore<B>) -> Self {
        Self {
            store,
            checks: BTreeMap::new(),
            writes: BTreeMap::new(),
            deadline: None,
        }
    }

    async fn read_many_raw(&mut self, keys: &[Vec<u8>]) -> Result<(), WorkspaceError> {
        let wanted = keys.to_vec();
        let unread = wanted
            .iter()
            .filter(|key| !self.checks.contains_key(*key))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        // Keep scoped compaction and other bounded topology transitions in
        // the same 4096-record envelope as the administrative census. A
        // freshly materialized layer may legitimately carry more than one
        // thousand delta rows; rejecting it at 1024 would make the scoped
        // path less capable than the legacy full-census path it replaces.
        if self.checks.len().saturating_add(unread.len()) > 4096 {
            return Err(WorkspaceError::InvalidReadPlan(
                "topology transaction authority limit".into(),
            ));
        }
        let mut values = Vec::new();
        for part in unread.chunks(32) {
            // A bounded TiKV point read may report a pre-submit key-lock
            // conflict after it has exhausted its per-request data slots.
            // The backend maps that case to Busy and guarantees that no write
            // was submitted. Rebuild this read packet before the surrounding
            // topology CAS continues; otherwise seal/fork callers would
            // surface a transient lock as a terminal operation failure.
            let mut attempt = 0;
            let (rows, _) = loop {
                let result = self
                    .store
                    .backend
                    .get_many_consistent_with_time_bounded(part, topology_point_limits(part.len()))
                    .await;
                match result {
                    Err(WorkspaceError::Busy) if attempt + 1 < CAS_MAX_RETRIES => {
                        retry_backoff(attempt).await;
                        attempt += 1;
                    }
                    Err(WorkspaceError::Busy) => return Err(WorkspaceError::Busy),
                    other => break other?,
                }
            };
            values.extend(rows);
        }
        if values.len() != unread.len() {
            return Err(WorkspaceError::CorruptMetadata(
                "backend returned the wrong number of values".into(),
            ));
        }
        for (key, value) in unread.into_iter().zip(values) {
            self.checks.insert(key, value);
        }
        Ok(())
    }

    async fn read_raw(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, WorkspaceError> {
        self.read_many_raw(std::slice::from_ref(&key)).await?;
        Ok(self.checks.get(&key).cloned().flatten())
    }

    async fn read<T: DeserializeOwned>(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Option<T>, WorkspaceError> {
        self.read_raw(key).await?.as_deref().map(decode).transpose()
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
        self.read(hot_journal_key(workspace, id)).await
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
        self.put_checked(put(key, value)?)
    }

    fn put_checked(&mut self, write: KvWrite) -> Result<(), WorkspaceError> {
        let key = match &write {
            KvWrite::Put { key, .. } | KvWrite::Delete { key } => key.clone(),
        };
        if !self.checks.contains_key(&key) {
            return Err(WorkspaceError::CorruptMetadata(
                "topology write has no declared read".into(),
            ));
        }
        self.writes.insert(key, write);
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
        self.put(hot_journal_key(row.workspace_id, row.journal_id), row)
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

    async fn commit(self) -> Result<bool, WorkspaceError> {
        let checks = self
            .checks
            .into_iter()
            .map(|(key, expected)| KvCheck { key, expected })
            .collect::<Vec<_>>();
        let writes = self.writes.into_values().collect::<Vec<_>>();
        // Every Busy reachable here is pre-submit: packet preparation saw a
        // shared root/inventory/topology epoch move (a sibling seal or fork),
        // or the backend rolled back before commit (TiKV exhausted key-lock
        // retries). No mutation exists, so report it as an ordinary CAS
        // conflict. Each caller's bounded retry loop then rebuilds from fresh
        // reads instead of every caller having to special-case Busy; only
        // exhausting that whole budget surfaces `workspace is busy`.
        let packet = match self
            .store
            .prepare_topology_packet(checks, writes, self.deadline)
            .await
        {
            Ok(packet) => packet,
            Err(WorkspaceError::Busy) => {
                tracing::debug!("topology packet preparation conflict; rebuilding");
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        match self.store.commit_prepared_topology_packet(&packet).await {
            Err(WorkspaceError::Busy) => {
                tracing::debug!("topology commit rolled back before submit; rebuilding");
                Ok(false)
            }
            result => result,
        }
    }
}

impl<B> KvWorkspaceStore<B>
where
    B: WorkspaceKvBackend,
{
    async fn allocate_id_block(&self, count: i64) -> Result<i64, WorkspaceError> {
        self.allocate_id_block_named("slice", count).await
    }

    /// Advance a durable allocator by `count` in one CAS and return the
    /// first reserved ID.
    async fn allocate_id_block_named(&self, name: &str, count: i64) -> Result<i64, WorkspaceError> {
        let key = hot_allocator_key(name);
        for _ in 0..CAS_MAX_RETRIES {
            let (raw, value) = self.load_hot::<i64>(key.clone()).await?;
            let current = value.unwrap_or(1);
            let next = current
                .checked_add(count)
                .ok_or_else(|| WorkspaceError::CorruptMetadata("allocator overflows".into()))?;
            let writes = [put(key.clone(), &next)?];
            let checks = [KvCheck {
                key: key.clone(),
                expected: raw,
            }];
            let packet = self
                .prepare_topology_envelope(checks.to_vec(), writes.to_vec(), None)
                .await?;
            if self.commit_prepared_topology_packet(&packet).await? {
                return Ok(current);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    pub fn new(backend: B) -> Self {
        Self {
            backend: Arc::new(backend),
            runtime_only: false,
            packed_reader_pin_budget: std::sync::OnceLock::new(),
            native_auxiliary_budget: std::sync::OnceLock::new(),
            slice_ids: IdBlock::default(),
            inode_ids: IdBlock::default(),
            inode_blocks: false,
            committed_head: Arc::default(),
            commit_view: Arc::default(),
        }
    }

    /// Mounted runtime only: reserve inode IDs 64 per allocator CAS.
    pub fn with_inode_id_blocks(mut self) -> Self {
        self.inode_blocks = true;
        self
    }

    pub fn from_arc(backend: Arc<B>) -> Self {
        Self {
            backend,
            runtime_only: false,
            packed_reader_pin_budget: std::sync::OnceLock::new(),
            native_auxiliary_budget: std::sync::OnceLock::new(),
            slice_ids: IdBlock::default(),
            inode_ids: IdBlock::default(),
            inode_blocks: false,
            committed_head: Arc::default(),
            commit_view: Arc::default(),
        }
    }

    /// Consume an administrator-capable store into an irreversible runtime
    /// scope before sharing it with a mount. No API widens an existing scope.
    /// The store itself is retained, preserving all mount proof identities.
    pub fn into_runtime(mut self) -> Self {
        self.runtime_only = true;
        self
    }

    fn require_admin_access(&self) -> Result<(), WorkspaceError> {
        if self.runtime_only {
            return Err(WorkspaceError::UnsupportedCapability(
                "operator runtime does not hold workspace management authority",
            ));
        }
        Ok(())
    }

    fn topology_txn(&self) -> TopologyTxn<'_, B> {
        TopologyTxn::new(self)
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

    async fn current_control(&self) -> Result<Option<ControlHeader>, WorkspaceError> {
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
        let Some(raw) = values.into_iter().next().flatten() else {
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
        self.ensure_catalog_migration_quiescent().await?;
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
        for entry in self.scan_entries(LEGACY_WORKSPACE_PREFIX.to_vec()).await? {
            let row: LegacyWorkspaceRecord = decode(&entry.value)?;
            state.workspaces.insert(row.workspace_id, row.into());
            checks.push(KvCheck {
                key: entry.key,
                expected: Some(entry.value),
            });
        }
        for entry in self.scan_entries(LEGACY_LAYER_PREFIX.to_vec()).await? {
            let row: LayerRecord = decode(&entry.value)?;
            state.layers.insert(row.layer_id, row);
            checks.push(KvCheck {
                key: entry.key,
                expected: Some(entry.value),
            });
        }
        for entry in self.scan_entries(LEGACY_LEASE_PREFIX.to_vec()).await? {
            let row: SnapshotLease = decode(&entry.value)?;
            state.leases.insert(row.lease_id, row);
            checks.push(KvCheck {
                key: entry.key,
                expected: Some(entry.value),
            });
        }
        for entry in self.scan_entries(LEGACY_SNAPSHOT_PREFIX.to_vec()).await? {
            let row: SnapshotRecord = decode(&entry.value)?;
            state.snapshots.insert(row.snapshot_id, row);
            checks.push(KvCheck {
                key: entry.key,
                expected: Some(entry.value),
            });
        }
        for entry in self.scan_entries(LEGACY_ALLOCATOR_PREFIX.to_vec()).await? {
            let name = String::from_utf8(
                entry
                    .key
                    .strip_prefix(LEGACY_ALLOCATOR_PREFIX)
                    .ok_or(WorkspaceError::Fenced)?
                    .to_vec(),
            )
            .map_err(|_| WorkspaceError::Fenced)?;
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
            writes.push(put(hot_journal_key(row.workspace_id, *id), row)?);
            writes.push(put(journal_index_key(*id), &row.workspace_id)?);
        }
        for (name, value) in &state.allocators {
            writes.push(put(allocator_key(name), value)?);
        }
        let marker = encode_migration(state)?;
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
                    &[
                        KvCheck {
                            key: CONTROL_KEY.to_vec(),
                            expected: Some(marker.clone()),
                        },
                        check,
                    ],
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
        self.read_complete_topology_census().await
    }

    async fn load_control(&self) -> Result<ControlState, WorkspaceError> {
        Ok(self.load_control_raw().await?.1)
    }

    async fn update_control<R, F>(&self, mut operation: F) -> Result<R, WorkspaceError>
    where
        F: FnMut(&mut ControlState, &mut Vec<KvWrite>) -> Result<R, WorkspaceError>,
    {
        self.update_control_with_packed_roots(false, |state, _, writes| operation(state, writes))
            .await
    }

    async fn update_control_with_packed_roots<R, F>(
        &self,
        protect_packed_roots: bool,
        operation: F,
    ) -> Result<R, WorkspaceError>
    where
        F: FnMut(
            &mut ControlState,
            &BTreeSet<LayerId>,
            &mut Vec<KvWrite>,
        ) -> Result<R, WorkspaceError>,
    {
        self.update_control_with_packed_roots_and_checks(protect_packed_roots, &[], operation)
            .await
    }

    async fn update_control_with_packed_roots_and_checks<R, F>(
        &self,
        protect_packed_roots: bool,
        extra_checks: &[KvCheck],
        operation: F,
    ) -> Result<R, WorkspaceError>
    where
        F: FnMut(
            &mut ControlState,
            &BTreeSet<LayerId>,
            &mut Vec<KvWrite>,
        ) -> Result<R, WorkspaceError>,
    {
        self.update_control_with_packed_roots_checks_and_native_absence(
            protect_packed_roots,
            extra_checks,
            None,
            operation,
        )
        .await
    }

    async fn update_control_with_packed_roots_checks_and_native_absence<R, F>(
        &self,
        protect_packed_roots: bool,
        extra_checks: &[KvCheck],
        native_grant_workspace: Option<WorkspaceId>,
        mut operation: F,
    ) -> Result<R, WorkspaceError>
    where
        F: FnMut(
            &mut ControlState,
            &BTreeSet<LayerId>,
            &mut Vec<KvWrite>,
        ) -> Result<R, WorkspaceError>,
    {
        for attempt in 0..CAS_MAX_RETRIES {
            // Capture before hydration. Layer creation/removal advances this
            // durable fence in the same CAS, even if CONTROL later byte-ABAs.
            let inventory_generation = self.backend.get(LAYER_INVENTORY_GENERATION_KEY).await?;
            layer_inventory_generation(&inventory_generation)?;
            let (packed_roots, packed_checks, _packed_reader_pin_admission) =
                if protect_packed_roots {
                    self.scan_packed_binding_roots().await?
                } else {
                    (BTreeSet::new(), Vec::new(), Vec::new())
                };
            let (raw, mut state, hot) = match self.load_control_raw().await {
                Ok(census) => census,
                Err(WorkspaceError::Busy) if native_grant_workspace.is_none() => {
                    // No mutation has been submitted. A concurrent topology
                    // change invalidated the census authentication; rebuild it
                    // before evaluating the operation and retained authorities.
                    retry_backoff(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            // Finalization must retain both the pre-scan authorities and
            // their inventory incarnation across every retry. Raw layer
            // bytes alone cannot distinguish deletion and identical recreation.
            for check in extra_checks {
                let current = if check.key.as_slice() == LAYER_INVENTORY_GENERATION_KEY {
                    inventory_generation.clone()
                } else if check.key.as_slice() == CONTROL_KEY {
                    // Packed carrier deletion retains the exact catalog
                    // header; compare it with this attempt's census read.
                    raw.clone()
                } else if check.key.starts_with(HOT_LAYER_PREFIX) {
                    hot.get(&check.key).cloned().unwrap_or(None)
                } else if self.is_carrier_deletion_check(&check.key)
                    || check.key.as_slice() == VOLUME_HEADER_KEY
                    || native_finalization::is_state_key(&check.key)
                    || native_finalization::is_reverse_state_key(&check.key)
                {
                    // These metadata bytes are validated by this same final
                    // CAS; no earlier read grants carrier cleanup authority.
                    check.expected.clone()
                } else {
                    return Err(WorkspaceError::CorruptMetadata(
                        "invalid finalization inventory authority".into(),
                    ));
                };
                if current != check.expected {
                    return Err(WorkspaceError::Busy);
                }
            }
            let before = state.clone();
            let mut writes = Vec::new();
            let result = operation(&mut state, &packed_roots, &mut writes)?;
            append_hot_diff(&before, &state, &mut writes)?;
            append_recovery_diff(&before, &state, &mut writes)?;
            // Existing-layer field changes keep this fence unchanged.
            if !before.layers.keys().eq(state.layers.keys()) {
                let next = next_layer_inventory_generation(&inventory_generation)?;
                writes.push(put(LAYER_INVENTORY_GENERATION_KEY.to_vec(), &next)?);
            }
            if before.header != state.header {
                writes.push(put_control(&ControlHeader {
                    schema_version: state.schema_version,
                    header: state.header.clone(),
                    catalog_format: CATALOG_FORMAT,
                })?);
            }
            append_entity_indexes(&before, &state, &mut writes)?;
            let mut checks = vec![KvCheck {
                key: LAYER_INVENTORY_GENERATION_KEY.to_vec(),
                expected: inventory_generation,
            }];
            // CONTROL is an immutable catalog header during ordinary entity
            // transitions. Keep it out of the CAS lock set unless this packet
            // actually changes the header; packet preparation still validates
            // the current header without granting a cross-workspace lock.
            if before.header != state.header {
                checks.push(KvCheck {
                    key: CONTROL_KEY.to_vec(),
                    expected: raw.clone(),
                });
            }
            checks.extend(hot.iter().map(|(key, expected)| KvCheck {
                key: key.clone(),
                expected: expected.clone(),
            }));
            for check in extra_checks {
                if let Some(existing) = checks.iter().find(|existing| existing.key == check.key) {
                    if existing.expected != check.expected {
                        return Err(WorkspaceError::Busy);
                    }
                } else {
                    // In particular, a missing target needs an exact None
                    // condition so a later creation cannot be finalized here.
                    checks.push(check.clone());
                }
            }
            checks.extend(packed_checks);
            add_missing_write_checks(self, &mut checks, &writes).await?;
            for write in &writes {
                let key = match write {
                    KvWrite::Put { key, .. } | KvWrite::Delete { key } => key,
                };
                if is_hot_key(key) && !checks.iter().any(|check| check.key == *key) {
                    checks.push(KvCheck {
                        key: key.clone(),
                        expected: hot.get(key).cloned().unwrap_or(None),
                    });
                } else if (key.as_slice() == VOLUME_HEADER_KEY
                    || key.starts_with(OPEN_RECOVERY_PREFIX))
                    && !checks.iter().any(|check| check.key == *key)
                {
                    checks.push(KvCheck {
                        key: key.clone(),
                        expected: self.backend.get(key).await?,
                    });
                }
            }
            if let Some(workspace) = native_grant_workspace {
                // These exact absences participate in the genuine grant CAS.
                // A concurrent first packed install/fork cannot interleave a
                // plain writer with its mandatory scoped writer incarnation.
                for key in [
                    packed_current_key(workspace),
                    packed_claim_key(workspace),
                    packed_history_key(workspace, 1),
                    packed_writer_authority::packed_writer_key(workspace),
                ] {
                    checks.push(KvCheck {
                        key,
                        expected: None,
                    });
                }
            }
            let packet = match self.prepare_topology_packet(checks, writes, None).await {
                Ok(packet) => packet,
                Err(WorkspaceError::Busy) if native_grant_workspace.is_none() => {
                    // Native derived-index preparation can lose its read-side
                    // epoch overlap. Only this pre-submission Busy rebuilds;
                    // an uncertain mutation response must propagate unchanged.
                    retry_backoff(attempt).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if self.commit_prepared_topology_packet(&packet).await? {
                return Ok(result);
            }
            if native_grant_workspace.is_some() {
                // A new packed grant uses the scoped joint session path. This
                // native call grants nothing and never retries across its birth.
                return Err(WorkspaceError::Busy);
            }
            // Layer membership updates contend on the shared inventory and
            // topology generations. A bare yield retries all concurrent
            // compactions in lockstep and can exhaust the bounded CAS budget
            // before Redis/TiKV has a chance to serialize the writers.
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn hydrate_hot_state(
        &self,
        _state: &mut ControlState,
        _raw: &mut BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> Result<(), WorkspaceError> {
        Err(WorkspaceError::CorruptMetadata(
            "legacy hot hydration is unavailable in the entity catalog".into(),
        ))
    }

    async fn load_hot<T: DeserializeOwned>(
        &self,
        key: Vec<u8>,
    ) -> Result<(Option<Vec<u8>>, Option<T>), WorkspaceError> {
        let (values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&[key], topology_point_limits(1))
            .await?;
        if values.len() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let raw = values.into_iter().next().flatten();
        let value = raw.as_deref().map(decode).transpose()?;
        Ok((raw, value))
    }

    async fn hot_mutation<R, F>(&self, guard: &HeadGuard, operation: F) -> Result<R, WorkspaceError>
    where
        F: FnMut(&mut LayerRecord, &mut Vec<KvWrite>) -> Result<R, WorkspaceError>,
    {
        self.hot_mutation_for_version(guard, None, operation).await
    }

    async fn hot_mutation_for_version<R, F>(
        &self,
        guard: &HeadGuard,
        expected: Option<&[LayerRecord; 2]>,
        mut operation: F,
    ) -> Result<R, WorkspaceError>
    where
        F: FnMut(&mut LayerRecord, &mut Vec<KvWrite>) -> Result<R, WorkspaceError>,
    {
        let workspace_key = hot_workspace_key(guard.workspace_id);
        let layer_key = hot_layer_key(guard.expected_head_layer_id);
        let lease_key = hot_lease_key(guard.workspace_id, guard.lease_id);
        for _ in 0..CAS_MAX_RETRIES {
            let mut keys = vec![workspace_key.clone(), layer_key.clone(), lease_key.clone()];
            if let Some(expected) = expected {
                keys.push(hot_layer_key(expected[1].layer_id));
            }
            let (values, now) = self.backend.get_many_with_time(&keys).await?;
            let mut values = values.into_iter();
            let workspace_raw = values.next().flatten();
            let layer_raw = values.next().flatten();
            let lease_raw = values.next().flatten();
            let base_raw = values.next().flatten();
            let workspace = workspace_raw.as_deref().map(decode).transpose()?;
            let layer = layer_raw.as_deref().map(decode).transpose()?;
            let lease = lease_raw.as_deref().map(decode).transpose()?;
            let workspace =
                workspace.ok_or(WorkspaceError::WorkspaceNotFound(guard.workspace_id))?;
            let mut layer = layer.ok_or(WorkspaceError::Fenced)?;
            let lease = lease.ok_or(WorkspaceError::Fenced)?;
            checked_hot_guard(&workspace, &layer, &lease, guard, now)?;
            if let Some(expected) = expected {
                let base: Option<LayerRecord> = base_raw.as_deref().map(decode).transpose()?;
                if layer != expected[0] || base.as_ref() != Some(&expected[1]) {
                    // A policy decision based on old attrs/ACLs must be
                    // recalculated by the caller, never replayed here.
                    return Err(WorkspaceError::Busy);
                }
            }
            let mut writes = Vec::new();
            let result = operation(&mut layer, &mut writes)?;
            let _hole_compaction = self
                .compact_obsolete_holes(layer.layer_id, &mut writes)
                .await?;
            writes.push(put(layer_key.clone(), &layer)?);
            let mut checks = vec![
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
            if let Some(expected) = expected {
                checks.push(KvCheck {
                    key: hot_layer_key(expected[1].layer_id),
                    expected: base_raw,
                });
            }
            let _writer = self
                .prepare_ordinary_writer_checks(guard, &lease, &mut checks)
                .await?;
            let deadline = Some(_writer.deadline.map_or(lease.expires_at_ns, |deadline| {
                deadline.min(lease.expires_at_ns)
            }));
            let packet = self
                .prepare_topology_packet(checks, writes, deadline)
                .await?;
            let committed = self.commit_prepared_topology_packet(&packet).await?;
            if committed {
                return Ok(result);
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn now_ns(&self) -> Result<i64, WorkspaceError> {
        self.backend.server_time_ns().await
    }

    async fn read_v3_open_context(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<V3OpenContext, WorkspaceError> {
        let workspace_key = hot_workspace_key(workspace_id);
        let sidecar_key = open_v3_key(workspace_id);
        let (routing_values, _) = self
            .backend
            .get_many_consistent_with_time_bounded(
                std::slice::from_ref(&workspace_key),
                v3_open_read_limits(),
            )
            .await?;
        if routing_values.len() != 1 {
            return Err(WorkspaceError::Backend(
                "short v3 workspace open routing read".into(),
            ));
        }
        let routing = routing_values.into_iter().next().flatten();
        let workspace: WorkspaceRecord = routing
            .as_deref()
            .map(|raw| decode_open_value(raw, OPEN_RECORD_MAX_BYTES))
            .transpose()?
            .ok_or(WorkspaceError::WorkspaceNotFound(workspace_id))?;
        if workspace.workspace_id != workspace_id {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open workspace key/record disagree".into(),
            ));
        }
        // Route only the base and selected history key. No routing value
        // authorizes a sidecar write; the final read includes every authority.
        let routing_keys = vec![
            workspace_key.clone(),
            hot_layer_key(workspace.head_layer_id),
            packed_current_key(workspace_id),
        ];
        let (routed, _) = self
            .backend
            .get_many_consistent_with_time_bounded(&routing_keys, v3_open_read_limits())
            .await?;
        if routed.len() != routing_keys.len() {
            return Err(WorkspaceError::Backend(
                "short v3 workspace open routing read".into(),
            ));
        }
        let routed_workspace: WorkspaceRecord = routed[0]
            .as_deref()
            .map(|raw| decode_open_value(raw, OPEN_RECORD_MAX_BYTES))
            .transpose()?
            .ok_or(WorkspaceError::WorkspaceNotFound(workspace_id))?;
        if routed_workspace.workspace_id != workspace_id {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open workspace key/record disagree".into(),
            ));
        }
        if routed_workspace.head_layer_id != workspace.head_layer_id {
            return Err(WorkspaceError::Busy);
        }
        let routed_head = decode_open_layer(&routed[1], "head")?;
        if routed_head.layer_id != workspace.head_layer_id {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open head key/record disagree".into(),
            ));
        }
        let base_id = routed_head.parent_layer_id.ok_or_else(|| {
            WorkspaceError::CorruptMetadata("v3 open head has no sealed base".into())
        })?;
        let routed_binding = routed[2]
            .as_deref()
            .map(PackedLowerBindingRecord::decode)
            .transpose()?;
        let binding_version = routed_binding
            .as_ref()
            .map_or(1, |record| record.binding.binding_version);
        // The open path uses immutable volume metadata and workspace-scoped
        // recovery state. It must never transfer the growing global CONTROL
        // topology document.
        let mut keys = vec![
            VOLUME_HEADER_KEY.to_vec(),
            workspace_key,
            routing_keys[1].clone(),
            hot_layer_key(base_id),
            sidecar_key,
            routing_keys[2].clone(),
            packed_claim_key(workspace_id),
            packed_history_key(workspace_id, 1),
        ];
        let history_index = if binding_version == 1 {
            7
        } else {
            keys.push(packed_history_key(workspace_id, binding_version));
            8
        };
        let recovery_index = keys.len();
        keys.push(open_v3_recovery_key(workspace_id));
        let (values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, v3_open_read_limits())
            .await?;
        if values.len() != keys.len() {
            return Err(WorkspaceError::Backend(
                "short v3 workspace open read".into(),
            ));
        }
        let header: VolumeHeader = values[0]
            .as_deref()
            .map(|raw| decode_open_value(raw, OPEN_RECORD_MAX_BYTES))
            .transpose()?
            .ok_or_else(|| {
                WorkspaceError::CorruptMetadata(
                    "v3 open requires an initialized volume header".into(),
                )
            })?;
        let mut state = ControlState {
            schema_version: header.schema_version,
            header: Some(header),
            ..ControlState::default()
        };
        if state.schema_version != WORKSPACE_SCHEMA_VERSION {
            return Err(WorkspaceError::UnsupportedSchemaVersion(
                state.schema_version,
            ));
        }
        let observed: WorkspaceRecord = values[1]
            .as_deref()
            .map(|raw| decode_open_value(raw, OPEN_RECORD_MAX_BYTES))
            .transpose()?
            .ok_or(WorkspaceError::WorkspaceNotFound(workspace_id))?;
        if observed.workspace_id != workspace_id {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open workspace key/record disagree".into(),
            ));
        }
        if observed.head_layer_id != workspace.head_layer_id {
            return Err(WorkspaceError::Busy);
        }
        let head = decode_open_layer(&values[2], "head")?;
        if head.layer_id != observed.head_layer_id {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open head key/record disagree".into(),
            ));
        }
        if head.parent_layer_id != Some(base_id) || values[5] != routed[2] {
            return Err(WorkspaceError::Busy);
        }
        let base = decode_open_layer(&values[3], "base")?;
        if base.layer_id != base_id {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open base key/record disagree".into(),
            ));
        }
        let packed_binding =
            decode_packed_pair(workspace_id, &values[5], &values[6], &values[history_index])?;
        if let Some(record) = &packed_binding {
            if record.binding.binding_version != binding_version {
                return Err(WorkspaceError::CorruptMetadata(
                    "v3 open PWB3 history key/record disagree".into(),
                ));
            }
            // Keep the initial history key as an absence sentinel and validate
            // its identity even when the current binding uses a later version.
            let first =
                PackedLowerBindingRecord::decode(values[7].as_deref().ok_or_else(|| {
                    WorkspaceError::CorruptMetadata(
                        "v3 open PWB3 initial history is missing".into(),
                    )
                })?)?;
            if first.workspace_id != workspace_id || first.binding.binding_version != 1 {
                return Err(WorkspaceError::CorruptMetadata(
                    "v3 open PWB3 initial history key/record disagree".into(),
                ));
            }
        }
        let recovery_required = match values[recovery_index].as_deref() {
            Some(raw) => {
                let record: V3RecoveryRecord = decode_open_value(raw, OPEN_RECOVERY_MAX_BYTES)?;
                if record.workspace_id != workspace_id {
                    return Err(WorkspaceError::CorruptMetadata(
                        "v3 open recovery key/record disagree".into(),
                    ));
                }
                record.incomplete
            }
            // A Sealing workspace without its recovery marker is incomplete
            // metadata and is rejected by topology validation below.
            None => false,
        };
        state.workspaces.insert(workspace_id, observed);
        state.layers.insert(head.layer_id, head);
        state.layers.insert(base.layer_id, base);
        let current: Option<V3OpenRecord> = values[4]
            .as_deref()
            .map(|raw| decode_open_value(raw, OPEN_RECORD_MAX_BYTES))
            .transpose()?;
        if let Some(record) = &current {
            validate_open_record(record, workspace_id)?;
        }
        let checks = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect();
        Ok(V3OpenContext {
            state,
            current,
            packed_binding,
            recovery_required,
            checks,
            now,
        })
    }

    async fn read_v3_open_record(
        &self,
        key: &[u8],
    ) -> Result<(V3OpenRecord, KvCheck, i64), WorkspaceError> {
        let keys = [key.to_vec()];
        let (mut values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(&keys, v3_open_read_limits())
            .await?;
        if values.len() != 1 {
            return Err(WorkspaceError::Backend("short v3 open record read".into()));
        }
        let raw = values.pop().flatten();
        let record: V3OpenRecord = decode_open_value(
            raw.as_deref().ok_or(WorkspaceError::Fenced)?,
            OPEN_RECORD_MAX_BYTES,
        )?;
        validate_open_record(&record, record.workspace_id)?;
        if open_v3_key(record.workspace_id) != key {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open key/record disagree".into(),
            ));
        }
        Ok((
            record,
            KvCheck {
                key: key.to_vec(),
                expected: raw,
            },
            now,
        ))
    }

    /// Claim the authoritative v3 open record for a workspace.
    pub async fn open_workspace_v3(
        &self,
        workspace_id: WorkspaceId,
        owner_id: impl Into<String>,
        ttl: Duration,
    ) -> Result<V3OpenToken, WorkspaceError> {
        let owner_id = owner_id.into();
        if owner_id.trim().is_empty() || owner_id.len() > OPEN_OWNER_MAX_BYTES {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open owner id must contain 1..256 bytes".into(),
            ));
        }
        let ttl_ns = duration_ns(ttl)?;
        if ttl_ns <= 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open ttl must be positive".into(),
            ));
        }
        let key = open_v3_key(workspace_id);
        for _ in 0..CAS_MAX_RETRIES {
            let V3OpenContext {
                state,
                current,
                packed_binding,
                recovery_required: context_recovery_required,
                mut checks,
                now,
            } = self.read_v3_open_context(workspace_id).await?;
            validate_v3_open_topology(
                &state,
                workspace_id,
                packed_binding.as_ref(),
                context_recovery_required,
            )?;
            let expires_at_ns = now
                .checked_add(ttl_ns)
                .ok_or_else(|| WorkspaceError::Backend("v3 open expiry overflow".into()))?;
            let deadline = current
                .as_ref()
                .filter(|record| record.expires_at_ns > now)
                .map_or(expires_at_ns, |record| {
                    record.expires_at_ns.min(expires_at_ns)
                });
            let (generation, state, recovery_required) = match current {
                Some(current) => {
                    if current.generation == 0 {
                        return Err(WorkspaceError::CorruptMetadata(
                            "v3 open generation must be positive".into(),
                        ));
                    }
                    if current.expires_at_ns > now && current.owner_id != owner_id {
                        return Err(WorkspaceError::Busy);
                    }
                    if current.expires_at_ns > now {
                        let recovery_required =
                            current.recovery_required || context_recovery_required;
                        (
                            current.generation,
                            if recovery_required {
                                V3OpenState::Recovering
                            } else {
                                V3OpenState::Ready
                            },
                            recovery_required,
                        )
                    } else {
                        let generation = current.generation.checked_add(1).ok_or_else(|| {
                            WorkspaceError::CorruptMetadata("v3 open generation overflow".into())
                        })?;
                        (
                            generation,
                            if context_recovery_required {
                                V3OpenState::Recovering
                            } else {
                                V3OpenState::Ready
                            },
                            context_recovery_required,
                        )
                    }
                }
                None => {
                    let recovery_required = context_recovery_required;
                    (
                        1,
                        if recovery_required {
                            V3OpenState::Recovering
                        } else {
                            V3OpenState::Ready
                        },
                        recovery_required,
                    )
                }
            };
            let record = V3OpenRecord {
                workspace_id,
                owner_id: owner_id.clone(),
                generation,
                expires_at_ns,
                state,
                recovery_required,
            };
            let mut writes = vec![put(key.clone(), &record)?];
            let _writer_owner = self
                .prepare_public_packed_open_transition(
                    workspace_id,
                    context_recovery_required,
                    &mut checks,
                    &mut writes,
                )
                .await?;
            let packet = self
                .prepare_topology_envelope(checks, writes, Some(deadline))
                .await?;
            if self.commit_prepared_topology_packet(&packet).await? {
                return Ok(V3OpenToken {
                    workspace_id,
                    owner_id,
                    generation,
                    expires_at_ns,
                    state,
                    recovery_required,
                });
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    /// Mark a v3 open token ready after seal recovery has completed.
    pub async fn mark_workspace_v3_ready(
        &self,
        token: &V3OpenToken,
    ) -> Result<V3OpenToken, WorkspaceError> {
        let key = open_v3_key(token.workspace_id);
        for _ in 0..CAS_MAX_RETRIES {
            let V3OpenContext {
                state,
                current,
                packed_binding,
                recovery_required,
                mut checks,
                now,
            } = self.read_v3_open_context(token.workspace_id).await?;
            validate_v3_open_topology(
                &state,
                token.workspace_id,
                packed_binding.as_ref(),
                recovery_required,
            )?;
            if recovery_required {
                return Err(WorkspaceError::Busy);
            }
            let current = current.ok_or(WorkspaceError::Fenced)?;
            check_v3_open_token(&current, token, now)?;
            let ready = V3OpenRecord {
                state: V3OpenState::Ready,
                recovery_required: false,
                ..current.clone()
            };
            let _writer_owner = self
                .prepare_public_open_idle_writer_check(token.workspace_id, &mut checks)
                .await?;
            let writes = vec![put(key.clone(), &ready)?];
            let packet = self
                .prepare_topology_envelope(checks, writes, Some(current.expires_at_ns))
                .await?;
            if self.commit_prepared_topology_packet(&packet).await? {
                return Ok(V3OpenToken {
                    workspace_id: token.workspace_id,
                    owner_id: ready.owner_id,
                    generation: ready.generation,
                    expires_at_ns: ready.expires_at_ns,
                    state: ready.state,
                    recovery_required: ready.recovery_required,
                });
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    /// Extend an open token while preserving its generation.
    pub async fn renew_workspace_v3(
        &self,
        token: &V3OpenToken,
        ttl: Duration,
    ) -> Result<V3OpenToken, WorkspaceError> {
        let ttl_ns = duration_ns(ttl)?;
        if ttl_ns <= 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open ttl must be positive".into(),
            ));
        }
        let key = open_v3_key(token.workspace_id);
        for _ in 0..CAS_MAX_RETRIES {
            let (current, check, now) = self.read_v3_open_record(&key).await?;
            check_v3_open_token(&current, token, now)?;
            let expires_at_ns = now
                .checked_add(ttl_ns)
                .ok_or_else(|| WorkspaceError::Backend("v3 open expiry overflow".into()))?;
            let renewed = V3OpenRecord {
                expires_at_ns,
                ..current.clone()
            };
            let mut checks = vec![check];
            let _writer_owner = self
                .prepare_public_open_idle_writer_check(token.workspace_id, &mut checks)
                .await?;
            let writes = vec![put(key.clone(), &renewed)?];
            let packet = self
                .prepare_topology_envelope(
                    checks,
                    writes,
                    Some(current.expires_at_ns.min(expires_at_ns)),
                )
                .await?;
            if self.commit_prepared_topology_packet(&packet).await? {
                return Ok(V3OpenToken {
                    workspace_id: token.workspace_id,
                    owner_id: renewed.owner_id,
                    generation: renewed.generation,
                    expires_at_ns,
                    state: renewed.state,
                    recovery_required: renewed.recovery_required,
                });
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    /// Release an open token while preserving its generation history.
    pub async fn close_workspace_v3(&self, token: &V3OpenToken) -> Result<(), WorkspaceError> {
        let key = open_v3_key(token.workspace_id);
        for _ in 0..CAS_MAX_RETRIES {
            let (current, check, now) = self.read_v3_open_record(&key).await?;
            check_v3_open_token(&current, token, now)?;
            let deadline = current.expires_at_ns;
            let closed = V3OpenRecord {
                expires_at_ns: now,
                ..current
            };
            let mut checks = vec![check];
            let _writer_owner = self
                .prepare_public_open_idle_writer_check(token.workspace_id, &mut checks)
                .await?;
            let writes = vec![put(key.clone(), &closed)?];
            let packet = self
                .prepare_topology_envelope(checks, writes, Some(deadline))
                .await?;
            if self.commit_prepared_topology_packet(&packet).await? {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn scan<T: DeserializeOwned>(&self, prefix: Vec<u8>) -> Result<Vec<T>, WorkspaceError> {
        let mut rows = Vec::new();
        let mut after = None;
        let mut scanned_rows = 0usize;
        let mut scanned_bytes = 0usize;
        loop {
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(&prefix, after.as_deref(), generic_scan_limits())
                .await?;
            if page.is_empty() {
                break;
            }
            let page_bytes = validate_generic_scan_page(&page, &prefix, after.as_deref())?;
            scanned_rows = scanned_rows
                .checked_add(page.len())
                .ok_or(WorkspaceError::Fenced)?;
            scanned_bytes = scanned_bytes
                .checked_add(page_bytes)
                .ok_or(WorkspaceError::Fenced)?;
            if scanned_rows > GENERIC_SCAN_MAX_ROWS || scanned_bytes > GENERIC_SCAN_MAX_BYTES {
                return Err(WorkspaceError::InvalidReadPlan(
                    "generic metadata scan exceeds bounded budget".into(),
                ));
            }
            after = page.last().map(|entry| entry.key.clone());
            rows.extend(page.into_iter().map(|entry| decode(&entry.value)));
        }
        rows.into_iter().collect()
    }

    async fn scan_entries(&self, prefix: Vec<u8>) -> Result<Vec<KvEntry>, WorkspaceError> {
        let mut rows = Vec::new();
        let mut after = None;
        let mut scanned_rows = 0usize;
        let mut scanned_bytes = 0usize;
        loop {
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(&prefix, after.as_deref(), generic_scan_limits())
                .await?;
            if page.is_empty() {
                break;
            }
            let page_bytes = validate_generic_scan_page(&page, &prefix, after.as_deref())?;
            scanned_rows = scanned_rows
                .checked_add(page.len())
                .ok_or(WorkspaceError::Fenced)?;
            scanned_bytes = scanned_bytes
                .checked_add(page_bytes)
                .ok_or(WorkspaceError::Fenced)?;
            if scanned_rows > GENERIC_SCAN_MAX_ROWS || scanned_bytes > GENERIC_SCAN_MAX_BYTES {
                return Err(WorkspaceError::InvalidReadPlan(
                    "generic metadata scan exceeds bounded budget".into(),
                ));
            }
            after = page.last().map(|entry| entry.key.clone());
            rows.extend(page);
        }
        Ok(rows)
    }

    /// Read every persisted PWB3 binding record and retain the exact values
    /// for the destructive metadata CAS. Binding history is an independent
    /// root catalog, so a stale gc_snapshot must never be able to delete a
    /// native layer that became reachable while the collector was preparing
    /// its candidate list.
    async fn scan_packed_binding_roots(
        &self,
    ) -> Result<
        (
            BTreeSet<LayerId>,
            Vec<KvCheck>,
            Vec<crate::workspace_overlay::packed_v3::wire005::V3OwnedPermit>,
        ),
        WorkspaceError,
    > {
        let budget =
            self.packed_reader_pin_budget
                .get()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "packed binding GC memory budget",
                ))?;
        // Retain the owner through the final destructive CAS. This accounts
        // for all decoded PWB3 rows and their exact checks, including rows
        // returned by a later page after a short first page.
        let binding_owner = budget
            .admit(&[(
                crate::workspace_overlay::packed_v3::wire005::V3BudgetPool::Metadata,
                PACKED_BINDING_SCAN_ADMISSION_BYTES,
            )])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let generation_key = PACKED_ROOT_GENERATION_KEY.to_vec();
        let (generation_values, now) = self
            .backend
            .get_many_consistent_with_time_bounded(
                std::slice::from_ref(&generation_key),
                KvReadLimits {
                    max_records: 1,
                    max_key_bytes: 256,
                    max_value_bytes: 512,
                    max_total_bytes: 4096,
                    max_response_bytes: 16 << 10,
                    max_data_requests: 1,
                },
            )
            .await?;
        if generation_values.len() != 1 || now <= 0 {
            return Err(WorkspaceError::Fenced);
        }
        let generation = generation_values.into_iter().next().flatten();
        next_packed_root_generation(&generation)?;
        let generation_check = KvCheck {
            key: generation_key.clone(),
            expected: generation,
        };
        let mut roots = BTreeSet::new();
        let mut checks = vec![generation_check.clone()];
        let page_limits = KvReadLimits {
            max_records: PACKED_BINDING_SCAN_PAGE_RECORDS,
            max_key_bytes: 256,
            max_value_bytes: 12 << 10,
            max_total_bytes: 512 << 10,
            max_response_bytes: 512 << 10,
            max_data_requests: 64,
        };
        let mut rows = 0usize;
        let mut bytes = 0usize;
        for prefix in [b"packed/v3/history/".as_slice(), b"packed/v3/current/"] {
            let mut after = None;
            let mut pages = 0usize;
            loop {
                pages = pages.checked_add(1).ok_or(WorkspaceError::Busy)?;
                if pages > PACKED_BINDING_SCAN_MAX_PAGES {
                    return Err(WorkspaceError::Busy);
                }
                // Every page is fenced on the same root generation. A new
                // binding can appear after a prior page and is then rejected
                // by this CAS before its roots are used for deletion.
                if !self
                    .backend
                    .compare_and_swap(std::slice::from_ref(&generation_check), &[])
                    .await?
                {
                    return Err(WorkspaceError::Busy);
                }
                let page = self
                    .backend
                    .scan_prefix_page_with_byte_limits(prefix, after.as_deref(), page_limits)
                    .await?;
                if !self
                    .backend
                    .compare_and_swap(std::slice::from_ref(&generation_check), &[])
                    .await?
                {
                    return Err(WorkspaceError::Busy);
                }
                if page.len() > PACKED_BINDING_SCAN_PAGE_RECORDS {
                    return Err(WorkspaceError::Fenced);
                }
                if page.is_empty() {
                    break;
                }
                for entry in page {
                    if !entry.key.starts_with(prefix)
                        || entry.key.len() > page_limits.max_key_bytes
                        || entry.value.len() > page_limits.max_value_bytes
                        || after.as_ref().is_some_and(|cursor| entry.key <= *cursor)
                    {
                        return Err(WorkspaceError::Fenced);
                    }
                    rows = rows.checked_add(1).ok_or(WorkspaceError::Busy)?;
                    bytes = bytes
                        .checked_add(entry.key.len())
                        .and_then(|total| total.checked_add(entry.value.len()))
                        .ok_or(WorkspaceError::Busy)?;
                    if rows > PACKED_BINDING_SCAN_MAX_ROWS || bytes > PACKED_BINDING_SCAN_MAX_BYTES
                    {
                        return Err(WorkspaceError::Busy);
                    }
                    let record = PackedLowerBindingRecord::decode(&entry.value)?;
                    let expected_key = if prefix == b"packed/v3/history/" {
                        packed_history_key(record.workspace_id, record.binding.binding_version)
                    } else {
                        packed_current_key(record.workspace_id)
                    };
                    if entry.key != expected_key {
                        return Err(WorkspaceError::CorruptMetadata(
                            "PWB3 GC key/record disagree".into(),
                        ));
                    }
                    roots.insert(record.base_revision.layer_id);
                    roots.insert(record.head_layer_id);
                    after = Some(entry.key.clone());
                    checks.push(KvCheck {
                        key: entry.key,
                        expected: Some(entry.value),
                    });
                }
            }
        }
        let reader_pins = self.packed_reader_pin_roots().await?;
        roots.extend(reader_pins.native_roots);
        checks.extend(reader_pins.checks);
        let (journal_roots, journal_checks, journal_admission) =
            self.scan_packed_journal_layer_roots().await?;
        roots.extend(journal_roots);
        checks.extend(journal_checks);
        let admissions = std::iter::once(binding_owner)
            .chain(reader_pins._permit)
            .chain(journal_admission)
            .collect();
        Ok((roots, checks, admissions))
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
    fn supports_packed_workspace_mount(&self) -> bool {
        self.backend.supports_consistent_reads() && self.packed_reader_pin_budget.get().is_some()
    }

    async fn shutdown_metadata_backend(&self) -> Result<(), WorkspaceError> {
        self.backend.shutdown_metadata_backend().await
    }

    async fn open_packed_reader_session(
        self: Arc<Self>,
        guard: HeadGuard,
        budget: Arc<crate::workspace_overlay::packed_v3::wire005::V3MountBudget>,
        options: crate::workspace_overlay::packed_reader_lifecycle::PackedReaderLeaseOptions,
    ) -> Result<
        Arc<dyn crate::workspace_overlay::packed_reader_lifecycle::PackedReaderSession>,
        WorkspaceError,
    > {
        let session =
            crate::workspace_overlay::packed_reader_lifecycle::KvPackedReaderSession::open(
                self, guard, budget, options,
            )
            .await?;
        Ok(session)
    }

    async fn reap_packed_reader_sessions(&self) -> Result<u64, WorkspaceError> {
        self.require_admin_access()?;
        self.reap_expired_packed_readers().await
    }

    fn supports_versioned_permissions(&self) -> bool {
        self.backend.supports_consistent_reads()
    }

    fn supports_packed_permissions(&self) -> bool {
        self.backend.supports_consistent_reads()
    }

    async fn validate_read_fence(
        &self,
        guard: HeadGuard,
        expected_layers: [LayerRecord; 2],
    ) -> Result<(), WorkspaceError> {
        let keys = vec![
            hot_workspace_key(guard.workspace_id),
            hot_layer_key(guard.expected_head_layer_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            hot_layer_key(expected_layers[0].layer_id),
            hot_layer_key(expected_layers[1].layer_id),
        ];
        let (values, now) = self.backend.get_many_consistent_with_time(&keys).await?;
        if values.len() != keys.len() {
            return Err(WorkspaceError::Backend(
                "short KV workspace read-fence read".into(),
            ));
        }
        let workspace: WorkspaceRecord = decode_required(&values[0])?;
        let head: LayerRecord = decode_required(&values[1])?;
        let lease: SnapshotLease = decode_required(&values[2])?;
        checked_hot_guard(&workspace, &head, &lease, &guard, now)?;
        let observed_head: LayerRecord = decode_required(&values[3])?;
        let observed_base: LayerRecord = decode_required(&values[4])?;
        if observed_head != expected_layers[0] || observed_base != expected_layers[1] {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }

    fn supports_fused_read_fence(&self) -> bool {
        self.backend.supports_consistent_reads()
    }

    fn take_committed_head(&self, head: LayerId) -> Option<LayerRecord> {
        self.committed_head
            .lock()
            .unwrap()
            .take()
            .filter(|record| record.layer_id == head)
    }

    async fn validate_inode_read_fence(
        &self,
        guard: HeadGuard,
        base: LayerRecord,
        ino: i64,
        expected: Vec<InodeDelta>,
    ) -> Result<(), WorkspaceError> {
        let (_, rows) = self.read_fenced_inode(guard, base, ino).await?;
        if rows != expected {
            return Err(WorkspaceError::Busy);
        }
        Ok(())
    }

    async fn read_fenced_inode(
        &self,
        guard: HeadGuard,
        base: LayerRecord,
        ino: i64,
    ) -> Result<(LayerRecord, Vec<InodeDelta>), WorkspaceError> {
        let keys = vec![
            hot_workspace_key(guard.workspace_id),
            hot_layer_key(guard.expected_head_layer_id),
            hot_lease_key(guard.workspace_id, guard.lease_id),
            inode_identity_key(guard.expected_head_layer_id, ino),
            inode_identity_key(base.layer_id, ino),
        ];
        let (values, now) = self.backend.get_many_consistent_with_time(&keys).await?;
        if values.len() != keys.len() {
            return Err(WorkspaceError::Backend(
                "short KV fused inode read-fence read".into(),
            ));
        }
        let workspace: WorkspaceRecord = decode_required(&values[0])?;
        let head: LayerRecord = decode_required(&values[1])?;
        let lease: SnapshotLease = decode_required(&values[2])?;
        checked_hot_guard(&workspace, &head, &lease, &guard, now)?;
        if head.parent_layer_id != Some(base.layer_id) {
            return Err(WorkspaceError::Fenced);
        }
        let mut rows = Vec::with_capacity(2);
        for (value, layer) in values[3..]
            .iter()
            .zip([guard.expected_head_layer_id, base.layer_id])
        {
            let Some(value) = value else { continue };
            let row: InodeDelta = decode(value)?;
            if row.layer_id != layer || row.ino != ino {
                return Err(WorkspaceError::CorruptMetadata(
                    "fused inode key/record identity mismatch".into(),
                ));
            }
            rows.push(row);
        }
        Ok((head, rows))
    }

    async fn get_extent_deltas_bounded(
        &self,
        request: ExtentQuery,
        max_rows: usize,
    ) -> Result<Vec<DataExtentDelta>, WorkspaceError> {
        if max_rows == 0 || max_rows > 1024 || request.layer_ids.len() != 2 {
            return Err(WorkspaceError::InvalidReadPlan(
                "invalid bounded extent query".into(),
            ));
        }
        if request.range_start > request.range_end {
            return Err(WorkspaceError::InvalidReadPlan(
                "extent query starts after its end".into(),
            ));
        }
        if request.range_start == request.range_end {
            return Ok(Vec::new());
        }

        let mut rows = Vec::with_capacity(max_rows);
        for layer in request.layer_ids {
            let remaining = max_rows - rows.len();
            let entries = self
                .backend
                .scan_prefix_bounded(
                    &extent_chunk_prefix(layer, request.ino, request.chunk_index),
                    remaining + 1,
                )
                .await?;
            if entries.len() > remaining {
                return Err(WorkspaceError::InvalidReadPlan(
                    "upper extent row limit exceeded".into(),
                ));
            }
            let mut found = Vec::with_capacity(entries.len());
            for entry in entries {
                let row: DataExtentDelta = decode(&entry.value)?;
                row.validate()?;
                if row.layer_id != layer
                    || row.ino != request.ino
                    || row.chunk_index != request.chunk_index
                    || entry.key != extent_key(&row)
                {
                    return Err(WorkspaceError::CorruptMetadata(
                        "bounded extent key/record identity mismatch".into(),
                    ));
                }
                let row_end = row.logical_offset + row.length;
                if row.logical_offset < request.range_end && row_end > request.range_start {
                    found.push(row);
                }
            }
            found.sort_by_key(|row| std::cmp::Reverse(row.sequence));
            if found.len() > remaining {
                return Err(WorkspaceError::InvalidReadPlan(
                    "upper extent row limit exceeded".into(),
                ));
            }
            rows.extend(found);
        }
        Ok(rows)
    }

    async fn load_packed_lower_binding(
        &self,
        guard: HeadGuard,
    ) -> Result<Option<PackedLowerBinding>, WorkspaceError> {
        Ok(self
            .load_packed_binding_record(guard)
            .await?
            .map(|record| record.binding))
    }

    async fn load_packed_binding_record(
        &self,
        guard: HeadGuard,
    ) -> Result<Option<PackedLowerBindingRecord>, WorkspaceError> {
        // Own both snapshots, decoded routing/authentication records and the
        // exact checks until final authentication. Registry checks retain their
        // separate permit on this same canonical ledger.
        let budget =
            self.packed_reader_pin_budget
                .get()
                .ok_or(WorkspaceError::UnsupportedCapability(
                    "packed binding read memory budget",
                ))?;
        let _binding_owner = budget
            .admit(&[(
                crate::workspace_overlay::packed_v3::wire005::V3BudgetPool::Metadata,
                1 << 20,
            )])
            .map_err(|error| WorkspaceError::InvalidReadPlan(error.to_string()))?;
        let limits = super::kv_backend::KvReadLimits {
            max_records: 8,
            max_key_bytes: 256,
            max_value_bytes: 12 << 10,
            max_total_bytes: 128 << 10,
            max_response_bytes: 256 << 10,
            max_data_requests: 8,
        };
        // A value conflict invalidates the entire captured authentication packet.
        // Re-read all rows and the live lease deadline within the fixed retry cap.
        for _ in 0..CAS_MAX_RETRIES {
            let mut keys = vec![
                hot_workspace_key(guard.workspace_id),
                hot_layer_key(guard.expected_head_layer_id),
                hot_lease_key(guard.workspace_id, guard.lease_id),
                packed_current_key(guard.workspace_id),
                packed_claim_key(guard.workspace_id),
                packed_history_key(guard.workspace_id, 1),
            ];
            let (first, now) = self
                .backend
                .get_many_consistent_with_time_bounded(&keys, limits)
                .await?;
            if first.len() != keys.len() {
                return Err(WorkspaceError::Backend("short PWB3 consistent read".into()));
            }
            let workspace: WorkspaceRecord = decode_required(&first[0])?;
            let head: LayerRecord = decode_required(&first[1])?;
            let lease: SnapshotLease = decode_required(&first[2])?;
            checked_hot_guard(&workspace, &head, &lease, &guard, now)?;
            let Some(current_bytes) = first[3].as_deref() else {
                return decode_packed_pair(guard.workspace_id, &first[3], &first[4], &first[5]);
            };
            // Current is only a routing hint. Initial history remains an absence
            // sentinel; the selected version is authenticated in the final read.
            let record = PackedLowerBindingRecord::decode(current_bytes)?;
            let base_id = head.parent_layer_id.ok_or(WorkspaceError::Fenced)?;
            let history_index = if record.binding.binding_version == 1 {
                5
            } else {
                keys.push(packed_history_key(
                    guard.workspace_id,
                    record.binding.binding_version,
                ));
                6
            };
            let base_index = keys.len();
            keys.push(hot_layer_key(base_id));
            // The final read contains every authority and the base in one version.
            // Nothing from the first routing read authorizes the returned binding.
            let (values, now) = self
                .backend
                .get_many_consistent_with_time_bounded(&keys, limits)
                .await?;
            if values.len() != keys.len() {
                return Err(WorkspaceError::Backend("short PWB3 final read".into()));
            }
            let workspace: WorkspaceRecord = decode_required(&values[0])?;
            let head: LayerRecord = decode_required(&values[1])?;
            let lease: SnapshotLease = decode_required(&values[2])?;
            checked_hot_guard(&workspace, &head, &lease, &guard, now)?;
            if head.parent_layer_id != Some(base_id) || values[3] != first[3] {
                return Err(WorkspaceError::Fenced);
            }
            let base: LayerRecord = decode_required(&values[base_index])?;
            if base.layer_id != base_id {
                return Err(WorkspaceError::CorruptMetadata(
                    "PWB3 base key/record disagree".into(),
                ));
            }
            let current = decode_packed_pair(
                guard.workspace_id,
                &values[3],
                &values[4],
                &values[history_index],
            )?
            .ok_or(WorkspaceError::Fenced)?;
            let initial =
                PackedLowerBindingRecord::decode(values[5].as_deref().ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("PWB3 initial history is missing".into())
                })?)?;
            if initial.workspace_id != guard.workspace_id || initial.binding.binding_version != 1 {
                return Err(WorkspaceError::CorruptMetadata(
                    "PWB3 initial history key/record disagree".into(),
                ));
            }
            if current != record {
                return Err(WorkspaceError::Fenced);
            }
            current.validate_for_guard(&guard, &base)?;
            let (_registry_owner, registry_checks) =
                self.packed_registry_publication_checks(&current).await?;
            let mut checks = keys
                .into_iter()
                .zip(values)
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>();
            self.merge_borrowed_checks(&mut checks, registry_checks)?;
            let authentication_limits = super::kv_backend::KvReadLimits {
                max_records: 32,
                max_key_bytes: 256,
                max_value_bytes: 48 << 10,
                max_total_bytes: 2 << 20,
                max_response_bytes: 64 << 10,
                max_data_requests: 32,
            };
            if !self
                .backend
                .authenticate_checks_before_bounded(
                    &checks,
                    lease.expires_at_ns,
                    authentication_limits,
                )
                .await?
            {
                tokio::task::yield_now().await;
                continue;
            }
            return Ok(Some(current));
        }
        Err(WorkspaceError::Busy)
    }

    async fn load_packed_binding_version(
        &self,
        workspace_id: WorkspaceId,
        version: u64,
    ) -> Result<Option<PackedLowerBindingRecord>, WorkspaceError> {
        let bytes = self
            .backend
            .get(&packed_history_key(workspace_id, version))
            .await?;
        let record = bytes
            .as_deref()
            .map(PackedLowerBindingRecord::decode)
            .transpose()?;
        if record.as_ref().is_some_and(|record| {
            record.workspace_id != workspace_id || record.binding.binding_version != version
        }) {
            return Err(WorkspaceError::CorruptMetadata(
                "PWB3 history key/record disagree".into(),
            ));
        }
        Ok(record)
    }

    async fn install_packed_lower_binding(
        &self,
        request: InstallPackedLowerBinding,
    ) -> Result<PackedLowerBindingRecord, WorkspaceError> {
        self.require_admin_access()?;
        let record = request.record()?;
        let encoded = record.encode()?;
        let keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(request.guard.workspace_id),
            hot_layer_key(request.guard.expected_head_layer_id),
            hot_lease_key(request.guard.workspace_id, request.guard.lease_id),
            hot_layer_key(request.expected_base.layer_id),
            hot_allocator_key("inode"),
            packed_current_key(request.guard.workspace_id),
            packed_claim_key(request.guard.workspace_id),
            packed_history_key(request.guard.workspace_id, 1),
            inode_identity_key(request.expected_base.layer_id, 1),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
        ];
        let (_registry_owner, registry_checks) =
            self.packed_registry_publication_checks(&record).await?;
        let (values, now) = self.backend.get_many_consistent_with_time(&keys).await?;
        if values.len() != keys.len() || values[0].is_none() {
            return Err(WorkspaceError::Backend(
                "missing/short PWB3 topology read".into(),
            ));
        }
        validate_current_control_raw(values[0].as_deref())?;
        let mut workspace: WorkspaceRecord = decode_required(&values[1])?;
        let mut head: LayerRecord = decode_required(&values[2])?;
        let lease: SnapshotLease = decode_required(&values[3])?;
        let base: LayerRecord = decode_required(&values[4])?;
        checked_hot_guard(&workspace, &head, &lease, &request.guard, now)?;
        if head != request.expected_layers[0]
            || base != request.expected_layers[1]
            || revision_from_layer(&base)? != request.expected_base
        {
            return Err(WorkspaceError::Busy);
        }
        let current = decode_packed_pair(
            request.guard.workspace_id,
            &values[6],
            &values[7],
            &values[8],
        )?;
        if current != request.expected_binding {
            return Err(WorkspaceError::Busy);
        }
        let allocator: i64 = decode_required(&values[5])?;
        if allocator != 2 {
            return Err(WorkspaceError::UnsupportedCapability(
                "first packed binding requires unissued native inode IDs",
            ));
        }
        let root: InodeDelta = decode_required(&values[9])?;
        request.validate_native_root(&root)?;
        let root_generation = next_packed_root_generation(&values[10])?;
        workspace.head_epoch = record.head_epoch;
        workspace.updated_at_ns = now;
        allocate_layer_sequences(&mut head, 1)?;
        let floor = record
            .highest_inode
            .checked_add(1)
            .ok_or_else(|| WorkspaceError::CorruptMetadata("packed inode floor overflow".into()))?
            .max(2);
        let mut writes = vec![
            put(keys[1].clone(), &workspace)?,
            put(keys[2].clone(), &head)?,
            put(keys[5].clone(), &floor)?,
            KvWrite::Put {
                key: keys[6].clone(),
                value: encoded.clone(),
            },
            KvWrite::Put {
                key: keys[7].clone(),
                value: PACKED_CLAIM.to_vec(),
            },
            KvWrite::Put {
                key: keys[8].clone(),
                value: encoded,
            },
            put(keys[10].clone(), &root_generation)?,
        ];
        let mut checks: Vec<KvCheck> = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect();
        checks.extend(registry_checks);
        let _writer_owner = self
            .prepare_initial_packed_writer_authority(
                &record,
                &request.guard,
                &mut checks,
                &mut writes,
            )
            .await?;
        // One attempt only: a loser cannot replay the old request under a new
        // head/base/binding generation. Object uploads remain caller orphans.
        let _native_holds = self
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await?;
        let packet = self
            .prepare_topology_envelope(checks, writes, Some(lease.expires_at_ns))
            .await?;
        if !self.commit_prepared_topology_packet(&packet).await? {
            return Err(WorkspaceError::Busy);
        }
        Ok(record)
    }

    async fn publish_packed_lower_binding(
        &self,
        request: PublishPackedLowerBinding,
    ) -> Result<PackedLowerBindingRecord, WorkspaceError> {
        self.require_admin_access()?;
        let record = request.record()?;
        let encoded = record.encode()?;
        let old_version = request.expected_binding.binding.binding_version;
        let old_history_key = packed_history_key(request.guard.workspace_id, old_version);
        let new_history_key =
            packed_history_key(request.guard.workspace_id, record.binding.binding_version);
        let mut keys = vec![
            CONTROL_KEY.to_vec(),
            hot_workspace_key(request.guard.workspace_id),
            hot_layer_key(request.guard.expected_head_layer_id),
            hot_lease_key(request.guard.workspace_id, request.guard.lease_id),
            hot_layer_key(request.expected_base.layer_id),
            hot_allocator_key("inode"),
            packed_current_key(request.guard.workspace_id),
            packed_claim_key(request.guard.workspace_id),
            packed_history_key(request.guard.workspace_id, 1),
        ];
        let old_history_index = if old_version == 1 {
            8
        } else {
            keys.push(old_history_key);
            keys.len() - 1
        };
        let new_history_index = keys.len();
        keys.push(new_history_key);
        let root_generation_index = keys.len();
        keys.push(PACKED_ROOT_GENERATION_KEY.to_vec());
        let (_registry_owner, registry_checks) =
            self.packed_registry_publication_checks(&record).await?;
        let (values, now) = self.backend.get_many_consistent_with_time(&keys).await?;
        if values.len() != keys.len() || values[0].is_none() {
            return Err(WorkspaceError::Backend(
                "missing/short PWB3 publication topology read".into(),
            ));
        }
        validate_current_control_raw(values[0].as_deref())?;
        let mut workspace: WorkspaceRecord = decode_required(&values[1])?;
        let mut head: LayerRecord = decode_required(&values[2])?;
        let lease: SnapshotLease = decode_required(&values[3])?;
        let base: LayerRecord = decode_required(&values[4])?;
        if values[7].as_deref() != Some(PACKED_CLAIM) {
            return Err(WorkspaceError::CorruptMetadata(
                "PWB3 publication claim is missing or invalid".into(),
            ));
        }
        let current =
            PackedLowerBindingRecord::decode(values[6].as_deref().ok_or(WorkspaceError::Fenced)?)?;
        let initial = PackedLowerBindingRecord::decode(values[8].as_deref().ok_or_else(|| {
            WorkspaceError::CorruptMetadata("PWB3 initial history is missing".into())
        })?)?;
        if initial.workspace_id != request.guard.workspace_id
            || initial.binding.binding_version != 1
        {
            return Err(WorkspaceError::CorruptMetadata(
                "PWB3 initial history key/record disagree".into(),
            ));
        }
        let old_history =
            PackedLowerBindingRecord::decode(values[old_history_index].as_deref().ok_or_else(
                || WorkspaceError::CorruptMetadata("PWB3 current history is missing".into()),
            )?)?;
        if current == record {
            let target_guard = HeadGuard {
                expected_head_epoch: record.head_epoch,
                ..request.guard.clone()
            };
            checked_hot_guard(&workspace, &head, &lease, &target_guard, now)?;
            let committed = PackedLowerBindingRecord::decode(
                values[new_history_index].as_deref().ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("PWB3 committed history is missing".into())
                })?,
            )?;
            if committed != record {
                return Err(WorkspaceError::CorruptMetadata(
                    "PWB3 committed current/history record disagree".into(),
                ));
            }
            if old_history != request.expected_binding {
                return Err(WorkspaceError::Busy);
            }
            let allocator: i64 = decode_required(&values[5])?;
            let target_guard = request.validate_committed_state(
                &record,
                &[head.clone(), base.clone()],
                &revision_from_layer(&base)?,
                allocator,
            )?;
            checked_hot_guard(&workspace, &head, &lease, &target_guard, now)?;
            if values[root_generation_index]
                .as_deref()
                .map(decode::<u64>)
                .transpose()?
                .is_some_and(|generation| generation == 0)
            {
                return Err(WorkspaceError::CorruptMetadata(
                    "PWB3 root generation must be positive".into(),
                ));
            }
            let mut checks: Vec<KvCheck> = keys
                .into_iter()
                .zip(values)
                .map(|(key, expected)| KvCheck { key, expected })
                .collect();
            checks.extend(registry_checks);
            // The snapshot is only a proof candidate until every authority
            // key and the live lease deadline still match at this timed CAS.
            // Returning an already-committed record changes no root generation.
            if !self
                .backend
                .compare_and_swap_before(&checks, &[], lease.expires_at_ns)
                .await?
            {
                return Err(WorkspaceError::Busy);
            }
            return Ok(record);
        }
        checked_hot_guard(&workspace, &head, &lease, &request.guard, now)?;
        if head != request.expected_layers[0]
            || base != request.expected_layers[1]
            || revision_from_layer(&base)? != request.expected_base
        {
            return Err(WorkspaceError::Busy);
        }
        if current != request.expected_binding {
            return Err(WorkspaceError::Busy);
        }
        if old_history != current {
            return Err(WorkspaceError::CorruptMetadata(
                "PWB3 current/history record disagree".into(),
            ));
        }
        if values[new_history_index].is_some() {
            return Err(WorkspaceError::Busy);
        }
        let root_generation = next_packed_root_generation(&values[root_generation_index])?;
        workspace.head_epoch = record.head_epoch;
        workspace.updated_at_ns = now;
        allocate_layer_sequences(&mut head, 1)?;
        let current_allocator: i64 = decode_required(&values[5])?;
        request.validate_first_publication_allocator(current_allocator)?;
        let floor = record
            .highest_inode
            .checked_add(1)
            .ok_or_else(|| WorkspaceError::CorruptMetadata("packed inode floor overflow".into()))?
            .max(2);
        let mut writes = vec![
            put(keys[1].clone(), &workspace)?,
            put(keys[2].clone(), &head)?,
            put(keys[5].clone(), &current_allocator.max(floor))?,
            KvWrite::Put {
                key: keys[6].clone(),
                value: encoded.clone(),
            },
            KvWrite::Put {
                key: keys[new_history_index].clone(),
                value: encoded,
            },
            put(keys[root_generation_index].clone(), &root_generation)?,
        ];
        let mut checks: Vec<KvCheck> = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect();
        checks.extend(registry_checks);
        let _native_holds = self
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await?;
        let packet = self
            .prepare_topology_envelope(checks, writes, Some(lease.expires_at_ns))
            .await?;
        if !self.commit_prepared_topology_packet(&packet).await? {
            return Err(WorkspaceError::Busy);
        }
        Ok(record)
    }

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

    fn gc_target_limit(&self) -> Option<usize> {
        self.backend
            .native_gc_metadata_page_quota()
            .map(|_| native_finalization::MAX_TARGETS)
    }

    async fn initialize_workspace_schema(&self) -> Result<(), WorkspaceError> {
        self.require_admin_access()?;
        self.migrate().await
    }

    async fn load_volume_header(&self) -> Result<Option<VolumeHeader>, WorkspaceError> {
        Ok(self
            .current_control()
            .await?
            .and_then(|header| header.header))
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
        if name == "slice" {
            // Slice IDs only need uniqueness (no publication floor reads
            // them). Reserve a block per durable CAS; unused IDs in a block
            // are simply skipped after a restart.
            if let Some(id) = self.slice_ids.take() {
                return Ok(id);
            }
            let first = self.allocate_id_block(SLICE_ID_BLOCK).await?;
            self.slice_ids.refill(first, SLICE_ID_BLOCK);
            return self.slice_ids.take().ok_or(WorkspaceError::Busy);
        }
        if name == "inode" && self.inode_blocks {
            // Packed publication takes max(allocator, highest native inode +
            // 1) as its floor, so a reserved block only leaves gaps; the
            // durable counter already covers every ID handed out from it.
            if let Some(id) = self.inode_ids.take() {
                return Ok(id);
            }
            let first = self
                .allocate_id_block_named("inode", INODE_ID_BLOCK)
                .await?;
            self.inode_ids.refill(first, INODE_ID_BLOCK);
            return self.inode_ids.take().ok_or(WorkspaceError::Busy);
        }
        self.allocate_id_block_named(name, 1).await
    }

    async fn create_volume_root(
        &self,
        request: CreateVolumeRoot,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        self.require_admin_access()?;
        let supported_header = (request.volume_format == VOLUME_FORMAT
            && request.schema_version == WORKSPACE_SCHEMA_VERSION)
            || (cfg!(feature = "native-packed-base")
                && request.volume_format == "workspace-native-v2"
                && request.schema_version == 2);
        if !supported_header {
            return Err(WorkspaceError::UnsupportedVolumeFormat(format!(
                "{}/{}",
                request.volume_format, request.schema_version
            )));
        }
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
                volume_format: request.volume_format.clone(),
                schema_version: request.schema_version,
                volume_id: request.volume_id,
                created_at_ns: now,
            });
            txn.put_checked(put_control(&control)?)?;
            txn.read_raw(VOLUME_HEADER_KEY.to_vec()).await?;
            txn.put(
                VOLUME_HEADER_KEY.to_vec(),
                control.header.as_ref().ok_or(WorkspaceError::Fenced)?,
            )?;
            txn.read_raw(inode_key(&root_inode)).await?;
            txn.put(inode_key(&root_inode), &root_inode)?;
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
        self.require_admin_access()?;
        self.create_workspace_with_carrier_expectation(request, false)
            .await
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
        self.require_admin_access()?;
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
        self.load_hot(hot_snapshot_key(id))
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
        self.require_admin_access()?;
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
            txn.deadline = Some(expires);
            for key in [
                packed_current_key(request.workspace_id),
                packed_claim_key(request.workspace_id),
                packed_history_key(request.workspace_id, 1),
                packed_writer_authority::packed_writer_key(request.workspace_id),
            ] {
                if txn.read_raw(key).await?.is_some() {
                    return Err(WorkspaceError::Busy);
                }
            }
            txn.put_workspace(&workspace)?;
            txn.put_lease(&lease)?;
            txn.put(lease_index_key(request.lease_id), &request.workspace_id)?;
            let committed = txn.commit().await?;
            if committed {
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
                .get_many_consistent_with_time_bounded(
                    std::slice::from_ref(&key),
                    topology_point_limits(1),
                )
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
            let original = lease.clone();
            lease.expires_at_ns = checked_expiry(now, request.ttl_ns)?;
            lease.updated_at_ns = now;
            let writes = [put(key.clone(), &lease)?];
            let mut checks = vec![
                KvCheck {
                    key: key.clone(),
                    expected: raw,
                },
                KvCheck {
                    key: lease_index_key(request.lease_id),
                    expected: Some(encode(&workspace_id)?),
                },
            ];
            let mut writes = writes.to_vec();
            let writer = self
                .prepare_plain_lease_transition(&original, &mut checks, &mut writes, false)
                .await?;
            let packet = self
                .prepare_topology_packet(
                    checks,
                    writes,
                    Some(writer.deadline.map_or(original.expires_at_ns, |deadline| {
                        deadline.min(original.expires_at_ns)
                    })),
                )
                .await?;
            if self.commit_prepared_topology_packet(&packet).await? {
                return Ok(lease);
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn release_clean_packed_shutdown(
        self: Arc<Self>,
        proof: crate::workspace_overlay::packed_shutdown::VerifiedCleanPackedShutdown,
    ) -> Result<(), WorkspaceError> {
        KvWorkspaceStore::release_clean_packed_mount(&self, proof).await
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
            let original = lease.clone();
            lease.state = LeaseState::Released;
            lease.updated_at_ns = now;
            txn.put_lease(&lease)?;
            let mut checks = txn
                .checks
                .into_iter()
                .map(|(key, expected)| KvCheck { key, expected })
                .collect::<Vec<_>>();
            let mut writes = txn.writes.into_values().collect::<Vec<_>>();
            let writer = self
                .prepare_plain_lease_transition(&original, &mut checks, &mut writes, true)
                .await?;
            let packet = self
                .prepare_topology_packet(checks, writes, writer.deadline)
                .await?;
            if self.commit_prepared_topology_packet(&packet).await? {
                return Ok(());
            }
            retry_backoff(attempt).await;
        }
        Err(WorkspaceError::Busy)
    }

    async fn reap_expired_leases(&self) -> Result<u64, WorkspaceError> {
        self.require_admin_access()?;
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

    async fn get_dentry_delta_page(
        &self,
        layer: LayerId,
        parent: i64,
        after_name: Option<&[u8]>,
        budget: Arc<crate::workspace_overlay::packed_v3::wire005::V3MountBudget>,
    ) -> Result<WorkspaceNamePage<DentryDelta>, WorkspaceError> {
        self.bounded_dentry_page(layer, parent, after_name, budget)
            .await
    }

    async fn get_layer_dentry_delta_page(
        &self,
        layer: LayerId,
        after: Option<(i64, &[u8])>,
        budget: Arc<super::super::packed_v3::wire005::V3MountBudget>,
    ) -> Result<WorkspaceNamePage<DentryDelta>, WorkspaceError> {
        self.bounded_layer_dentry_page(layer, after, budget).await
    }

    async fn get_native_reverse_authority(
        &self,
        layers: &[LayerRecord],
        budget: Arc<super::super::packed_v3::wire005::V3MountBudget>,
    ) -> Result<WorkspaceNativeReverseAuthority, WorkspaceError> {
        self.native_reverse_authority(layers, budget).await
    }

    async fn get_native_reverse_dentry_page(
        &self,
        authority: &WorkspaceNativeReverseAuthority,
        layer: LayerId,
        ino: i64,
        after: Option<(i64, &[u8])>,
        budget: Arc<super::super::packed_v3::wire005::V3MountBudget>,
    ) -> Result<WorkspaceNamePage<DentryDelta>, WorkspaceError> {
        self.native_reverse_page(authority, layer, ino, after, budget)
            .await
    }

    async fn confirm_native_reverse_authority(
        &self,
        authority: &WorkspaceNativeReverseAuthority,
        budget: Arc<super::super::packed_v3::wire005::V3MountBudget>,
    ) -> Result<(), WorkspaceError> {
        self.confirm_native_reverse(authority, budget).await
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

    async fn read_permission_snapshot(
        &self,
        request: PermissionSnapshotQuery,
    ) -> Result<PermissionSnapshot, WorkspaceError> {
        use crate::meta::posix_acl::{ACCESS_XATTR, DEFAULT_XATTR};
        if request.inodes.is_empty()
            || request.inodes.len() > 2
            || request.inodes.iter().any(|ino| *ino <= 0)
            || (request.inodes.len() == 2 && request.inodes[0] == request.inodes[1])
            || request.layer_ids[0] == request.layer_ids[1]
        {
            return Err(WorkspaceError::CorruptMetadata(
                "invalid bounded permission query".into(),
            ));
        }
        if let Some((parent, name)) = &request.dentry {
            DentryDelta::put(request.layer_ids[0], *parent, name.clone(), 1, 0, 0).validate()?;
        }
        let mut keys = request
            .layer_ids
            .iter()
            .map(|layer| hot_layer_key(*layer))
            .collect::<Vec<_>>();
        for ino in &request.inodes {
            for layer in &request.layer_ids {
                keys.push(inode_identity_key(*layer, *ino));
                for name in [ACCESS_XATTR, DEFAULT_XATTR, b"system.brewfs.acl".as_slice()] {
                    keys.push(xattr_identity_key(*layer, *ino, name));
                }
            }
        }
        if let Some((parent, name)) = &request.dentry {
            for layer in &request.layer_ids {
                keys.push(dentry_identity_key(*layer, *parent, name));
            }
        }
        let raw = self.backend.get_many_consistent(&keys).await?;
        if raw.len() != keys.len() {
            return Err(WorkspaceError::CorruptMetadata(
                "permission snapshot response length mismatch".into(),
            ));
        }
        let mut raw = raw.into_iter();
        let head: LayerRecord = raw
            .next()
            .flatten()
            .as_deref()
            .map(decode)
            .transpose()?
            .ok_or(WorkspaceError::LayerNotFound(request.layer_ids[0]))?;
        let base: LayerRecord = raw
            .next()
            .flatten()
            .as_deref()
            .map(decode)
            .transpose()?
            .ok_or(WorkspaceError::LayerNotFound(request.layer_ids[1]))?;
        let layers = [head, base];
        validate_permission_layers(&layers)?;
        if [layers[0].layer_id, layers[1].layer_id] != request.layer_ids {
            return Err(WorkspaceError::CorruptMetadata(
                "permission snapshot layer identity mismatch".into(),
            ));
        }
        let mut snapshot = PermissionSnapshot {
            layers,
            inodes: Vec::new(),
            xattrs: Vec::new(),
            dentries: Vec::new(),
        };
        for ino in request.inodes {
            for layer in request.layer_ids {
                if let Some(value) = raw.next().flatten() {
                    let row: InodeDelta = decode(&value)?;
                    if row.layer_id != layer || row.ino != ino {
                        return Err(WorkspaceError::CorruptMetadata(
                            "permission inode identity mismatch".into(),
                        ));
                    }
                    snapshot.inodes.push(row);
                }
                for name in [ACCESS_XATTR, DEFAULT_XATTR, b"system.brewfs.acl".as_slice()] {
                    if let Some(value) = raw.next().flatten() {
                        let row: XattrDelta = decode(&value)?;
                        if row.layer_id != layer || row.ino != ino || row.name != name {
                            return Err(WorkspaceError::CorruptMetadata(
                                "permission xattr identity mismatch".into(),
                            ));
                        }
                        snapshot.xattrs.push(row);
                    }
                }
            }
        }
        if let Some((parent, name)) = request.dentry {
            for layer in request.layer_ids {
                if let Some(value) = raw.next().flatten() {
                    let row: DentryDelta = decode(&value)?;
                    if row.layer_id != layer || row.parent_ino != parent || row.name != name {
                        return Err(WorkspaceError::CorruptMetadata(
                            "permission dentry identity mismatch".into(),
                        ));
                    }
                    snapshot.dentries.push(row);
                }
            }
        }
        Ok(snapshot)
    }

    async fn apply_permission_mutation(
        &self,
        request: PermissionMutation,
    ) -> Result<MutationResult, WorkspaceError> {
        use crate::meta::posix_acl::{ACCESS_XATTR, DEFAULT_XATTR, PosixAcl};
        validate_permission_layers(&request.expected_layers)?;
        let head = request.guard.expected_head_layer_id;
        if request.expected_layers[0].layer_id != head
            || request.inodes.is_empty()
            || request.inodes.len() > 2
            || request.dentries.len() > 1
            || request.xattrs.len() > 2
            || request.inodes.iter().any(|inode| inode.layer_id != head)
            || request
                .dentries
                .iter()
                .any(|dentry| dentry.layer_id != head)
            || request.xattrs.iter().any(|xattr| xattr.layer_id != head)
        {
            return Err(WorkspaceError::Fenced);
        }
        let mut identities = HashSet::new();
        if request
            .inodes
            .iter()
            .any(|inode| !identities.insert(inode.ino))
        {
            return Err(WorkspaceError::CorruptMetadata(
                "duplicate permission inode write".into(),
            ));
        }
        let mut names = HashSet::new();
        for xattr in &request.xattrs {
            validate_value(xattr.op, xattr.value.as_deref(), "POSIX ACL")?;
            let inode = request
                .inodes
                .iter()
                .find(|inode| inode.ino == xattr.ino)
                .ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("ACL write lacks its inode".into())
                })?;
            if !names.insert((xattr.ino, xattr.name.clone()))
                || (xattr.name != ACCESS_XATTR && xattr.name != DEFAULT_XATTR)
                || inode.kind == 2
                || inode.state != InodeState::Present
                || (xattr.name == DEFAULT_XATTR && inode.kind != 1)
            {
                return Err(WorkspaceError::CorruptMetadata(
                    "invalid permission ACL identity/kind".into(),
                ));
            }
            if let Some(value) = &xattr.value {
                let acl = PosixAcl::decode(value)
                    .map_err(|message| WorkspaceError::CorruptMetadata(message.into()))?;
                if xattr.name == ACCESS_XATTR && acl.mode_bits() != inode.mode & 0o777 {
                    return Err(WorkspaceError::CorruptMetadata(
                        "mode and access ACL disagree".into(),
                    ));
                }
            }
        }
        for dentry in &request.dentries {
            dentry.validate()?;
        }
        let count = request.inodes.len() + request.dentries.len() + request.xattrs.len();
        self.hot_mutation_for_version(
            &request.guard,
            Some(&request.expected_layers),
            |layer, writes| {
                let range = allocate_layer_sequences(layer, count)?
                    .expect("permission mutation has records");
                let mut sequence = range.0;
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
                for template in &request.inodes {
                    let mut row = template.clone();
                    row.sequence = sequence;
                    sequence += 1;
                    writes.push(put(inode_key(&row), &row)?);
                }
                Ok(MutationResult {
                    first_sequence: Some(range.0),
                    last_sequence: Some(range.1),
                })
            },
        )
        .await
    }

    async fn apply_versioned_mutation(
        &self,
        request: VersionedMutation,
    ) -> Result<MutationResult, WorkspaceError> {
        let count = request.validate()?;
        self.hot_mutation_for_version(
            &request.guard,
            Some(&request.expected_layers),
            |layer, writes| {
                let Some((first, last)) = allocate_layer_sequences(layer, count)? else {
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
                        layer.owned_slice_count =
                            layer.owned_slice_count.checked_add(1).ok_or_else(|| {
                                WorkspaceError::Backend("owned slice count overflows".into())
                            })?;
                        layer.owned_bytes =
                            layer.owned_bytes.checked_add(row.length).ok_or_else(|| {
                                WorkspaceError::Backend("owned byte count overflows".into())
                            })?;
                    }
                    writes.push(put(extent_key(&row), &row)?);
                }
                Ok(MutationResult {
                    first_sequence: Some(first),
                    last_sequence: Some(last),
                })
            },
        )
        .await
    }

    async fn read_packed_permission_snapshot(
        &self,
        guard: HeadGuard,
        binding: PackedLowerBinding,
        request: PermissionSnapshotQuery,
    ) -> Result<PermissionSnapshot, WorkspaceError> {
        self.read_packed_permission_snapshot_bounded(guard, binding, request)
            .await
    }

    async fn apply_packed_versioned_mutation(
        &self,
        request: VersionedMutation,
        binding: PackedLowerBinding,
    ) -> Result<MutationResult, WorkspaceError> {
        self.apply_packed_versioned_mutation_owned(request, binding)
            .await
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
        let mut scanned_rows_total = 0usize;
        let mut scanned_bytes_total = 0usize;
        for layer in request.layer_ids {
            let prefix = extent_chunk_prefix(layer, request.ino, request.chunk_index);
            let mut after = None;
            let mut found = Vec::new();
            loop {
                let page = self
                    .backend
                    .scan_prefix_page_with_byte_limits(
                        &prefix,
                        after.as_deref(),
                        delta_scan_limits(),
                    )
                    .await?;
                if page.is_empty() {
                    break;
                }
                let next = page.last().map(|entry| entry.key.clone());
                scanned_rows_total = scanned_rows_total
                    .checked_add(page.len())
                    .ok_or(WorkspaceError::Fenced)?;
                scanned_bytes_total = scanned_bytes_total
                    .checked_add(validate_delta_scan_page(&page, &prefix, after.as_deref())?)
                    .ok_or(WorkspaceError::Fenced)?;
                if scanned_rows_total > DELTA_SCAN_MAX_ROWS
                    || scanned_bytes_total > DELTA_SCAN_MAX_BYTES
                {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "extent delta scan exceeds bounded metadata budget".into(),
                    ));
                }
                for entry in page {
                    let row: DataExtentDelta = decode(&entry.value)?;
                    row.validate()?;
                    if row.layer_id != layer
                        || row.ino != request.ino
                        || row.chunk_index != request.chunk_index
                        || entry.key != extent_key(&row)
                    {
                        return Err(WorkspaceError::CorruptMetadata(
                            "extent key/record identity mismatch".into(),
                        ));
                    }
                    let row_end = row.logical_offset.saturating_add(row.length);
                    if row.logical_offset < request.range_end && row_end > request.range_start {
                        found.push(row);
                    }
                }
                after = next;
            }
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
            let values = self.backend.get_many(&keys).await?;
            if values.len() != keys.len() {
                return Err(WorkspaceError::Fenced);
            }
            let mut rows = Vec::new();
            for (key, value) in keys.into_iter().zip(values) {
                let Some(value) = value else { continue };
                let row: XattrDelta = decode(&value)?;
                if xattr_key(&row) != key {
                    return Err(WorkspaceError::CorruptMetadata(
                        "xattr key/record identity mismatch".into(),
                    ));
                }
                rows.push(row);
            }
            return Ok(rows);
        }
        let mut rows = Vec::new();
        let mut scanned_rows_total = 0usize;
        let mut scanned_bytes_total = 0usize;
        for layer in request.layer_ids {
            let prefix = xattr_inode_prefix(layer, request.ino);
            let mut after = None;
            let mut found = Vec::new();
            loop {
                let page = self
                    .backend
                    .scan_prefix_page_with_byte_limits(
                        &prefix,
                        after.as_deref(),
                        delta_scan_limits(),
                    )
                    .await?;
                if page.is_empty() {
                    break;
                }
                let next = page.last().map(|entry| entry.key.clone());
                scanned_rows_total = scanned_rows_total
                    .checked_add(page.len())
                    .ok_or(WorkspaceError::Fenced)?;
                scanned_bytes_total = scanned_bytes_total
                    .checked_add(validate_delta_scan_page(&page, &prefix, after.as_deref())?)
                    .ok_or(WorkspaceError::Fenced)?;
                if scanned_rows_total > DELTA_SCAN_MAX_ROWS
                    || scanned_bytes_total > DELTA_SCAN_MAX_BYTES
                {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "xattr delta scan exceeds bounded metadata budget".into(),
                    ));
                }
                for entry in page {
                    let row: XattrDelta = decode(&entry.value)?;
                    if row.layer_id != layer
                        || row.ino != request.ino
                        || entry.key != xattr_key(&row)
                    {
                        return Err(WorkspaceError::CorruptMetadata(
                            "xattr key/record identity mismatch".into(),
                        ));
                    }
                    found.push(row);
                }
                after = next;
            }
            found.sort_by(|left, right| left.name.cmp(&right.name));
            rows.extend(found);
        }
        Ok(rows)
    }

    async fn get_xattr_delta_page(
        &self,
        layer: LayerId,
        ino: i64,
        after_name: Option<&[u8]>,
        budget: Arc<crate::workspace_overlay::packed_v3::wire005::V3MountBudget>,
    ) -> Result<WorkspaceNamePage<XattrDelta>, WorkspaceError> {
        self.bounded_xattr_page(layer, ino, after_name, budget)
            .await
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
            let values = self.backend.get_many(&keys).await?;
            if values.len() != keys.len() {
                return Err(WorkspaceError::Fenced);
            }
            let mut rows = Vec::new();
            for (key, value) in keys.into_iter().zip(values) {
                let Some(value) = value else { continue };
                let row: AclDelta = decode(&value)?;
                if acl_key(&row) != key {
                    return Err(WorkspaceError::CorruptMetadata(
                        "ACL key/record identity mismatch".into(),
                    ));
                }
                rows.push(row);
            }
            return Ok(rows);
        }
        let mut rows = Vec::new();
        let mut scanned_rows_total = 0usize;
        let mut scanned_bytes_total = 0usize;
        for layer in request.layer_ids {
            let prefix = acl_inode_prefix(layer, request.ino);
            let mut after = None;
            let mut found = Vec::new();
            loop {
                let page = self
                    .backend
                    .scan_prefix_page_with_byte_limits(
                        &prefix,
                        after.as_deref(),
                        delta_scan_limits(),
                    )
                    .await?;
                if page.is_empty() {
                    break;
                }
                let next = page.last().map(|entry| entry.key.clone());
                scanned_rows_total = scanned_rows_total
                    .checked_add(page.len())
                    .ok_or(WorkspaceError::Fenced)?;
                scanned_bytes_total = scanned_bytes_total
                    .checked_add(validate_delta_scan_page(&page, &prefix, after.as_deref())?)
                    .ok_or(WorkspaceError::Fenced)?;
                if scanned_rows_total > DELTA_SCAN_MAX_ROWS
                    || scanned_bytes_total > DELTA_SCAN_MAX_BYTES
                {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "ACL delta scan exceeds bounded metadata budget".into(),
                    ));
                }
                for entry in page {
                    let row: AclDelta = decode(&entry.value)?;
                    if row.layer_id != layer || row.ino != request.ino || entry.key != acl_key(&row)
                    {
                        return Err(WorkspaceError::CorruptMetadata(
                            "ACL key/record identity mismatch".into(),
                        ));
                    }
                    found.push(row);
                }
                after = next;
            }
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

    async fn load_seal_journal(
        &self,
        journal_id: JournalId,
    ) -> Result<SealJournal, WorkspaceError> {
        let workspace_id = self.journal_workspace(journal_id).await?;
        self.load_hot(hot_journal_key(workspace_id, journal_id))
            .await?
            .1
            .ok_or_else(|| WorkspaceError::Backend(format!("seal journal not found: {journal_id}")))
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

    async fn mark_workspace_deleting(&self, request: MarkDeleting) -> Result<(), WorkspaceError> {
        self.require_admin_access()?;
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
            txn.read_raw(extent_key(&extent)).await?;
            txn.put(extent_key(&extent), &extent)?;
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
        self.require_admin_access()?;
        let state = self.load_control().await?;
        let lease_cutoff = now_ns.saturating_sub(u64_to_i64(lease_grace_ns, "lease grace")?);
        let mut roots = BTreeSet::new();
        for workspace in state.workspaces.values() {
            if workspace.state != WorkspaceState::Deleting {
                roots.insert(workspace.head_layer_id);
                if let Some(fork) = &workspace.fork_base {
                    roots.insert(fork.layer_id);
                }
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
        // PWB3 bindings, reader pins, and packed journal records are all
        // independent roots. Reuse the same bounded paginated census used by
        // destructive metadata CAS preparation so an observational GC
        // snapshot cannot materialize an unbounded Redis/TiKV namespace.
        let (packed_roots, _packed_checks, _packed_admissions) =
            self.scan_packed_binding_roots().await?;
        roots.extend(packed_roots);
        let mut layers = state.layers.into_values().collect::<Vec<_>>();
        layers.sort_by_key(|layer| (layer.created_at_ns, layer.layer_id));
        let extent_prefix = b"delta/extent/";
        let mut extents = Vec::new();
        let mut after = None;
        let mut scanned_rows_total = 0usize;
        let mut scanned_bytes_total = 0usize;
        loop {
            let page = self
                .backend
                .scan_prefix_page_with_byte_limits(
                    extent_prefix,
                    after.as_deref(),
                    delta_scan_limits(),
                )
                .await?;
            if page.is_empty() {
                break;
            }
            let next = page.last().map(|entry| entry.key.clone());
            scanned_rows_total = scanned_rows_total
                .checked_add(page.len())
                .ok_or(WorkspaceError::Fenced)?;
            scanned_bytes_total = scanned_bytes_total
                .checked_add(validate_delta_scan_page(
                    &page,
                    extent_prefix,
                    after.as_deref(),
                )?)
                .ok_or(WorkspaceError::Fenced)?;
            if scanned_rows_total > DELTA_SCAN_MAX_ROWS
                || scanned_bytes_total > DELTA_SCAN_MAX_BYTES
            {
                return Err(WorkspaceError::InvalidReadPlan(
                    "extent delta scan exceeds bounded metadata budget".into(),
                ));
            }
            for entry in page {
                let row: DataExtentDelta = decode(&entry.value)?;
                if entry.key != extent_key(&row) {
                    return Err(WorkspaceError::CorruptMetadata(
                        "extent key/record identity mismatch".into(),
                    ));
                }
                extents.push(row);
            }
            after = next;
        }
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
        self.require_admin_access()?;
        if request.layer_ids.is_empty() {
            return Ok(());
        }
        self.update_control_with_packed_roots(true, |state, packed_roots, _| {
            let lease_cutoff = request
                .now_ns
                .saturating_sub(u64_to_i64(request.lease_grace_ns, "lease grace")?);
            let mut reachable = reachable_layers(state, lease_cutoff);
            reachable.extend(reachable_from_roots(state, packed_roots.iter().copied()));
            if request
                .layer_ids
                .iter()
                .any(|layer| reachable.contains(layer))
            {
                return Err(WorkspaceError::Busy);
            }
            for layer_id in &request.layer_ids {
                if let Some(layer) = state.layers.get_mut(layer_id) {
                    if layer.state == LayerState::Deleting {
                        continue;
                    }
                    if layer.state != LayerState::Sealed {
                        return Err(WorkspaceError::Busy);
                    }
                    layer.state = LayerState::Deleting;
                    layer.owner_workspace_id = None;
                }
            }
            Ok(())
        })
        .await
    }

    async fn reserve_gc_slice_deletion(
        &self,
        slice_id: u64,
        retained_slice_end: u64,
        deleted_layers: &[LayerId],
    ) -> Result<(), WorkspaceError> {
        self.reserve_native_slice_deletion(slice_id, retained_slice_end, deleted_layers)
            .await
    }

    async fn reserve_gc_slice_deletions(
        &self,
        slices: &[(u64, u64)],
        deleted_layers: &[LayerId],
    ) -> Result<(), WorkspaceError> {
        self.reserve_native_slice_deletions(slices, deleted_layers)
            .await
    }

    async fn gc_slice_deletion_reservation_complete(
        &self,
        slice_id: u64,
        retained_slice_end: u64,
        deleted_layers: &[LayerId],
    ) -> Result<bool, WorkspaceError> {
        self.native_slice_deletion_reservation_complete(
            slice_id,
            retained_slice_end,
            deleted_layers,
        )
        .await
    }

    async fn finalize_layer_metadata_deletion(
        &self,
        layer_ids: Vec<LayerId>,
    ) -> Result<(), WorkspaceError> {
        self.require_admin_access()?;
        if layer_ids.is_empty() {
            return Ok(());
        }
        if let Some(quota) = self.backend.native_gc_metadata_page_quota() {
            return self.finalize_native_metadata_pages(layer_ids, quota).await;
        }
        let mut keys = vec![LAYER_INVENTORY_GENERATION_KEY.to_vec()];
        keys.extend(layer_ids.iter().copied().map(hot_layer_key));
        let values = self.backend.get_many_consistent(&keys).await?;
        if values.len() != keys.len() {
            return Err(WorkspaceError::Backend(
                "short finalization layer-authority read".into(),
            ));
        }
        layer_inventory_generation(&values[0])?;
        for (layer_id, raw) in layer_ids.iter().zip(values.iter().skip(1)) {
            if let Some(raw) = raw {
                let layer: LayerRecord = decode(raw)?;
                if layer.layer_id != *layer_id {
                    return Err(WorkspaceError::CorruptMetadata(
                        "finalization layer key/record disagree".into(),
                    ));
                }
                if layer.state != LayerState::Deleting {
                    return Err(WorkspaceError::Busy);
                }
            }
        }
        let mut scanned_authorities: Vec<KvCheck> = keys
            .into_iter()
            .zip(values)
            .map(|(key, expected)| KvCheck { key, expected })
            .collect();
        let carrier_cleanup = self
            .prepare_carrier_metadata_deletion(&layer_ids, &scanned_authorities)
            .await?;
        self.merge_borrowed_checks(&mut scanned_authorities, carrier_cleanup.checks.clone())?;
        // Deleting forbids mutation before scanning. Retain this inventory
        // generation and every raw value, including missing layers; a later
        // incarnation cannot authorize deletion using these scanned keys.
        let mut entries = BTreeMap::<LayerId, Vec<Vec<u8>>>::new();
        for layer_id in &layer_ids {
            let mut keys = Vec::new();
            for prefix in [
                dentry_layer_prefix(*layer_id),
                inode_layer_prefix(*layer_id),
                xattr_layer_prefix(*layer_id),
                acl_layer_prefix(*layer_id),
                extent_layer_prefix(*layer_id),
                native_reverse::layer_prefix(*layer_id),
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
        self.update_control_with_packed_roots_and_checks(
            true,
            &scanned_authorities,
            |state, packed_roots, writes| {
                writes.extend(carrier_cleanup.writes.clone());
                let reachable = reachable_from_roots(state, packed_roots.iter().copied());
                if layer_ids.iter().any(|layer| reachable.contains(layer)) {
                    return Err(WorkspaceError::Busy);
                }
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
                    writes.push(KvWrite::Delete {
                        key: native_reverse::state_key(*layer_id),
                    });
                    if let Some(keys) = entries.get(layer_id) {
                        writes.extend(keys.iter().cloned().map(|key| KvWrite::Delete { key }));
                    }
                }
                Ok(())
            },
        )
        .await
    }

    async fn prune_terminal_records(
        &self,
        now_ns: i64,
        grace_ns: u64,
    ) -> Result<(), WorkspaceError> {
        self.require_admin_access()?;
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
                // Packed clean/recovery protocols retain historical leases as
                // exact attempt evidence. Their scale gate is not closed by
                // native terminal pruning.
                let mut retained = false;
                for key in [
                    packed_current_key(lease.workspace_id),
                    packed_claim_key(lease.workspace_id),
                    packed_history_key(lease.workspace_id, 1),
                    packed_writer_authority::packed_writer_key(lease.workspace_id),
                    format!(
                        "packed-v3/clean-release/{}/{}",
                        lease.workspace_id, lease.lease_id
                    )
                    .into_bytes(),
                    format!(
                        "packed-v3/mount-recovery/{}/{}",
                        lease.workspace_id, lease.lease_id
                    )
                    .into_bytes(),
                    format!("packed-v3/published-clean/{}", lease.workspace_id).into_bytes(),
                ] {
                    retained |= txn.read_raw(key).await?.is_some();
                }
                if retained {
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
                let candidate = (journal.updated_at_ns, journal.journal_id);
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
                let mut retained = false;
                for key in [
                    packed_current_key(journal.workspace_id),
                    packed_claim_key(journal.workspace_id),
                    packed_writer_authority::packed_writer_key(journal.workspace_id),
                    format!("packed/v3/native-freeze-basis/{}", journal.journal_id).into_bytes(),
                    format!("packed/v3/native-recovery-claim/{}", journal.journal_id).into_bytes(),
                ] {
                    retained |= txn.read_raw(key).await?.is_some();
                }
                if retained {
                    break;
                }
                txn.read_journal_index(journal.journal_id).await?;
                txn.delete(hot_journal_key(journal.workspace_id, journal.journal_id))?;
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

    #[cfg(test)]
    async fn install_test_base(
        &self,
        request: InstallTestBase,
    ) -> Result<TestBaseResult, WorkspaceError> {
        self.require_admin_access()?;
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
            let mut delta_writes = Vec::new();
            for row in &request.delta.dentries {
                delta_writes.push(put(dentry_key(row), row)?);
            }
            for row in &request.delta.inodes {
                delta_writes.push(put(inode_key(row), row)?);
            }
            for row in &request.delta.xattrs {
                delta_writes.push(put(xattr_key(row), row)?);
            }
            for row in &request.delta.acls {
                delta_writes.push(put(acl_key(row), row)?);
            }
            for row in &request.delta.extents {
                delta_writes.push(put(extent_key(row), row)?);
            }
            let keys = delta_writes
                .iter()
                .map(|write| match write {
                    KvWrite::Put { key, .. } | KvWrite::Delete { key } => key.clone(),
                })
                .collect::<Vec<_>>();
            txn.read_many_raw(&keys).await?;
            for write in delta_writes {
                txn.put_checked(write)?;
            }
            let committed = txn.commit().await?;
            if committed {
                return Ok(TestBaseResult {
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

fn open_v3_key(workspace_id: WorkspaceId) -> Vec<u8> {
    let mut key = OPEN_V3_PREFIX.to_vec();
    key.extend_from_slice(workspace_id.to_string().as_bytes());
    key
}

fn open_v3_recovery_key(workspace_id: WorkspaceId) -> Vec<u8> {
    let mut key = OPEN_RECOVERY_PREFIX.to_vec();
    key.extend_from_slice(workspace_id.to_string().as_bytes());
    key
}

fn duration_ns(duration: Duration) -> Result<i64, WorkspaceError> {
    i64::try_from(duration.as_nanos())
        .map_err(|_| WorkspaceError::CorruptMetadata("v3 open TTL overflows nanoseconds".into()))
}

fn decode_open_value<T: DeserializeOwned>(
    bytes: &[u8],
    max_bytes: usize,
) -> Result<T, WorkspaceError> {
    use bincode::Options;
    if bytes.len() > max_bytes {
        return Err(WorkspaceError::CorruptMetadata(
            "v3 open record exceeds decode limit".into(),
        ));
    }
    let payload = bytes.strip_prefix(ENVELOPE_MAGIC).ok_or_else(|| {
        WorkspaceError::CorruptMetadata("v3 open record has invalid envelope".into())
    })?;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(max_bytes as u64)
        .reject_trailing_bytes()
        .deserialize(payload)
        .map_err(|error| WorkspaceError::CorruptMetadata(format!("decode v3 open record: {error}")))
}

fn decode_open_layer(raw: &Option<Vec<u8>>, name: &str) -> Result<LayerRecord, WorkspaceError> {
    decode_open_value(
        raw.as_deref().ok_or_else(|| {
            WorkspaceError::CorruptMetadata(format!("v3 open {name} layer is missing"))
        })?,
        OPEN_RECORD_MAX_BYTES,
    )
}

fn validate_v3_open_topology(
    state: &ControlState,
    workspace_id: WorkspaceId,
    packed_binding: Option<&PackedLowerBindingRecord>,
    recovery_required: bool,
) -> Result<(), WorkspaceError> {
    if state.schema_version != WORKSPACE_SCHEMA_VERSION {
        return Err(WorkspaceError::UnsupportedSchemaVersion(
            state.schema_version,
        ));
    }
    let header = state.header.as_ref().ok_or_else(|| {
        WorkspaceError::CorruptMetadata("v3 open requires an initialized volume header".into())
    })?;
    if header.volume_format != VOLUME_FORMAT {
        return Err(WorkspaceError::UnsupportedVolumeFormat(
            header.volume_format.clone(),
        ));
    }
    if header.schema_version != WORKSPACE_SCHEMA_VERSION {
        return Err(WorkspaceError::UnsupportedSchemaVersion(
            header.schema_version,
        ));
    }
    let workspace = state
        .workspaces
        .get(&workspace_id)
        .ok_or(WorkspaceError::WorkspaceNotFound(workspace_id))?;
    let expected_head_state = match (workspace.state, recovery_required) {
        (WorkspaceState::Active, false) => LayerState::Writable,
        (WorkspaceState::Sealing, true) => LayerState::Sealing,
        _ => {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open workspace state has no matching writable or recovering head".into(),
            ));
        }
    };
    let head = state.layers.get(&workspace.head_layer_id).ok_or_else(|| {
        WorkspaceError::CorruptMetadata("v3 open workspace head layer is missing".into())
    })?;
    if head.schema_version != WORKSPACE_SCHEMA_VERSION {
        return Err(WorkspaceError::UnsupportedSchemaVersion(
            head.schema_version,
        ));
    }
    if head.state != expected_head_state
        || head.owner_workspace_id != Some(workspace_id)
        || head.next_sequence == 0
        || head.sealed_version.is_some()
        || head.delta_digest.is_some()
        || head.root_hash.is_some()
        || head.sealed_at_ns.is_some()
    {
        return Err(WorkspaceError::CorruptMetadata(
            "v3 open workspace head layer is invalid".into(),
        ));
    }
    let base_id = head
        .parent_layer_id
        .ok_or_else(|| WorkspaceError::CorruptMetadata("v3 open head has no sealed base".into()))?;
    let base = state.layers.get(&base_id).ok_or_else(|| {
        WorkspaceError::CorruptMetadata("v3 open sealed base layer is missing".into())
    })?;
    if base.schema_version != WORKSPACE_SCHEMA_VERSION {
        return Err(WorkspaceError::UnsupportedSchemaVersion(
            base.schema_version,
        ));
    }
    if base.owner_workspace_id.is_some()
        || base.next_sequence == 0
        || base.sealed_version.is_none_or(|version| version == 0)
        || base.sealed_at_ns.is_none()
    {
        return Err(WorkspaceError::CorruptMetadata(
            "v3 open sealed base layer is invalid".into(),
        ));
    }
    // The sidecar currently supports only the fixed head/base pair. A deeper
    // recovery chain is rejected until bounded recovery routing is integrated.
    let mut resolvable_head = head.clone();
    resolvable_head.state = LayerState::Writable;
    validate_layer_chain(workspace.head_layer_id, &[resolvable_head, base.clone()])?;
    if let Some(record) = packed_binding
        && (record.workspace_id != workspace_id
            || record.head_layer_id != head.layer_id
            || record.head_epoch != workspace.head_epoch
            || record.base_revision.layer_id != base.layer_id
            || Some(record.base_revision.sealed_version) != base.sealed_version
            || Some(record.base_revision.root_hash) != base.root_hash)
    {
        return Err(WorkspaceError::CorruptMetadata(
            "v3 open current PWB3 binding disagrees with head/base".into(),
        ));
    }
    Ok(())
}

fn workspace_has_incomplete_seal(state: &ControlState, workspace_id: WorkspaceId) -> bool {
    state.journals.values().any(|journal| {
        journal.workspace_id == workspace_id
            && !matches!(journal.phase, SealPhase::Completed | SealPhase::Aborted)
    })
}

fn check_v3_open_token(
    current: &V3OpenRecord,
    expected: &V3OpenToken,
    now: i64,
) -> Result<(), WorkspaceError> {
    if current.workspace_id != expected.workspace_id
        || current.owner_id != expected.owner_id
        || current.generation != expected.generation
        || current.expires_at_ns <= now
    {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
}

fn validate_open_record(
    record: &V3OpenRecord,
    workspace_id: WorkspaceId,
) -> Result<(), WorkspaceError> {
    if record.workspace_id != workspace_id
        || record.owner_id.trim().is_empty()
        || record.owner_id.len() > OPEN_OWNER_MAX_BYTES
        || record.generation == 0
        || (record.state == V3OpenState::Recovering) != record.recovery_required
    {
        return Err(WorkspaceError::CorruptMetadata(
            "invalid v3 open record identity/state".into(),
        ));
    }
    Ok(())
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

fn packed_current_key(workspace_id: WorkspaceId) -> Vec<u8> {
    format!("packed/v3/current/{}", workspace_id).into_bytes()
}

fn layer_inventory_generation(raw: &Option<Vec<u8>>) -> Result<u64, WorkspaceError> {
    let generation = raw.as_deref().map(decode::<u64>).transpose()?.unwrap_or(0);
    if raw.is_some() && generation == 0 {
        return Err(WorkspaceError::CorruptMetadata(
            "layer inventory generation must be positive".into(),
        ));
    }
    Ok(generation)
}

fn next_layer_inventory_generation(raw: &Option<Vec<u8>>) -> Result<u64, WorkspaceError> {
    layer_inventory_generation(raw)?
        .checked_add(1)
        .ok_or_else(|| {
            WorkspaceError::CorruptMetadata("layer inventory generation overflow".into())
        })
}

fn next_packed_root_generation(raw: &Option<Vec<u8>>) -> Result<u64, WorkspaceError> {
    let generation = raw.as_deref().map(decode::<u64>).transpose()?.unwrap_or(0);
    if raw.is_some() && generation == 0 {
        return Err(WorkspaceError::CorruptMetadata(
            "PWB3 root generation must be positive".into(),
        ));
    }
    generation
        .checked_add(1)
        .ok_or_else(|| WorkspaceError::CorruptMetadata("PWB3 root generation overflow".into()))
}

fn packed_claim_key(workspace_id: WorkspaceId) -> Vec<u8> {
    format!("packed/v3/claim/{}", workspace_id).into_bytes()
}

fn packed_history_key(workspace_id: WorkspaceId, version: u64) -> Vec<u8> {
    format!("packed/v3/history/{workspace_id}/{version:016x}").into_bytes()
}

fn decode_required<T: DeserializeOwned>(raw: &Option<Vec<u8>>) -> Result<T, WorkspaceError> {
    decode(raw.as_deref().ok_or(WorkspaceError::Fenced)?)
}

fn decode_packed_pair(
    workspace_id: WorkspaceId,
    current: &Option<Vec<u8>>,
    claim: &Option<Vec<u8>>,
    history: &Option<Vec<u8>>,
) -> Result<Option<PackedLowerBindingRecord>, WorkspaceError> {
    if current.is_none() && claim.is_none() && history.is_none() {
        return Ok(None);
    }
    if claim.as_deref() != Some(PACKED_CLAIM) || current.is_none() || current != history {
        return Err(WorkspaceError::CorruptMetadata(
            "PWB3 claim/current/history disagree".into(),
        ));
    }
    let record =
        PackedLowerBindingRecord::decode(current.as_deref().ok_or(WorkspaceError::Fenced)?)?;
    if record.workspace_id != workspace_id {
        return Err(WorkspaceError::CorruptMetadata(
            "PWB3 current key/record disagree".into(),
        ));
    }
    Ok(Some(record))
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

fn snapshot_key(id: SnapshotId) -> Vec<u8> {
    [SNAPSHOT_PREFIX, id.to_string().as_bytes()].concat()
}

fn snapshot_name_key(name: &str) -> Vec<u8> {
    [SNAPSHOT_NAME_PREFIX, hex::encode(name).as_bytes()].concat()
}

fn allocator_key(name: &str) -> Vec<u8> {
    [ALLOCATOR_PREFIX, name.as_bytes()].concat()
}

fn journal_prefix(workspace: WorkspaceId) -> Vec<u8> {
    format!("journal/{workspace}/").into_bytes()
}

fn journal_index_key(id: JournalId) -> Vec<u8> {
    [JOURNAL_INDEX_PREFIX, id.to_string().as_bytes()].concat()
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
    tokio::time::sleep(Duration::from_millis((base_ms + jitter_ms).min(100))).await;
}

fn hot_workspace_key(id: WorkspaceId) -> Vec<u8> {
    workspace_key(id)
}

fn hot_layer_key(id: LayerId) -> Vec<u8> {
    layer_key(id)
}

fn hot_lease_key(workspace: WorkspaceId, id: LeaseId) -> Vec<u8> {
    lease_key(workspace, id)
}

fn hot_journal_key(workspace: WorkspaceId, id: JournalId) -> Vec<u8> {
    format!("journal/{workspace}/{id}").into_bytes()
}

fn hot_lease_index_key(id: LeaseId) -> Vec<u8> {
    lease_index_key(id)
}

fn hot_journal_index_key(id: JournalId) -> Vec<u8> {
    journal_index_key(id)
}

fn hot_snapshot_key(id: SnapshotId) -> Vec<u8> {
    [HOT_SNAPSHOT_PREFIX, id.to_string().as_bytes()].concat()
}

fn hot_allocator_key(name: &str) -> Vec<u8> {
    allocator_key(name)
}

fn is_hot_key(key: &[u8]) -> bool {
    key.starts_with(HOT_WORKSPACE_PREFIX)
        || key.starts_with(HOT_LAYER_PREFIX)
        || key.starts_with(HOT_LEASE_PREFIX)
        || key.starts_with(HOT_SNAPSHOT_PREFIX)
        || key.starts_with(HOT_ALLOCATOR_PREFIX)
}

fn allocator_name_from_key(key: &[u8]) -> Result<String, WorkspaceError> {
    let name = key.strip_prefix(HOT_ALLOCATOR_PREFIX).ok_or_else(|| {
        WorkspaceError::CorruptMetadata("invalid hot allocator key prefix".into())
    })?;
    String::from_utf8(name.to_vec())
        .map_err(|_| WorkspaceError::CorruptMetadata("invalid hot allocator key".into()))
}

fn append_recovery_diff(
    before: &ControlState,
    after: &ControlState,
    writes: &mut Vec<KvWrite>,
) -> Result<(), WorkspaceError> {
    let mut workspaces = BTreeSet::new();
    workspaces.extend(before.journals.values().map(|journal| journal.workspace_id));
    workspaces.extend(after.journals.values().map(|journal| journal.workspace_id));
    for workspace_id in workspaces {
        let before_incomplete = workspace_has_incomplete_seal(before, workspace_id);
        let after_incomplete = workspace_has_incomplete_seal(after, workspace_id);
        if before_incomplete == after_incomplete {
            continue;
        }
        let key = open_v3_recovery_key(workspace_id);
        if after_incomplete {
            writes.push(put(
                key,
                &V3RecoveryRecord {
                    workspace_id,
                    incomplete: true,
                },
            )?);
        } else {
            writes.push(KvWrite::Delete { key });
        }
    }
    Ok(())
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
    append_map_diff(&before.snapshots, &after.snapshots, writes, |id| {
        hot_snapshot_key(*id)
    })?;
    append_map_diff(&before.allocators, &after.allocators, writes, |name| {
        hot_allocator_key(name)
    })?;
    for (id, row) in &before.leases {
        if !after.leases.contains_key(id) {
            writes.push(KvWrite::Delete {
                key: hot_lease_key(row.workspace_id, *id),
            });
        }
    }
    for (id, row) in &after.leases {
        if before.leases.get(id) != Some(row) {
            writes.push(put(hot_lease_key(row.workspace_id, *id), row)?);
        }
    }
    for (id, row) in &before.journals {
        if !after.journals.contains_key(id) {
            writes.push(KvWrite::Delete {
                key: hot_journal_key(row.workspace_id, *id),
            });
        }
    }
    for (id, row) in &after.journals {
        if before.journals.get(id) != Some(row) {
            writes.push(put(hot_journal_key(row.workspace_id, *id), row)?);
        }
    }
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
        || workspace.active_lease != Some(guard.lease_id)
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

fn validate_permission_layers(layers: &[LayerRecord; 2]) -> Result<(), WorkspaceError> {
    validate_layer_chain(layers[0].layer_id, layers)?;
    if layers[0].state != LayerState::Writable
        || layers[0].depth != 2
        || layers[0].parent_layer_id != Some(layers[1].layer_id)
        || layers[1].state != LayerState::Sealed
        || layers[1].depth != 1
        || layers[1].parent_layer_id.is_some()
    {
        return Err(WorkspaceError::Fenced);
    }
    Ok(())
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
            .flat_map(|workspace| {
                [
                    Some(workspace.head_layer_id),
                    workspace.fork_base.as_ref().map(|fork| fork.layer_id),
                ]
                .into_iter()
                .flatten()
            }),
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
    reachable_from_roots(state, pending)
}

fn reachable_from_roots(
    state: &ControlState,
    pending: impl IntoIterator<Item = LayerId>,
) -> HashSet<LayerId> {
    let mut pending = pending.into_iter().collect::<Vec<_>>();
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
pub(crate) use tests::packed_adapter_test_store;

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn operator_runtime_scope_rejects_management_without_backend_mutation() {
        let backend = MemoryBackend::default();
        let admin = budgeted_kv_store(backend.clone());
        admin.initialize_workspace_schema().await.unwrap();
        backend.cas_checks.lock().await.clear();
        backend.cas_write_keys.lock().await.clear();
        let runtime = Arc::new(admin.into_runtime());
        let identity = Arc::as_ptr(&runtime);
        let before = backend.records.lock().await.clone();
        fn denied<T>(result: Result<T, WorkspaceError>) {
            assert!(matches!(
                result,
                Err(WorkspaceError::UnsupportedCapability(_))
            ));
        }
        denied(runtime.initialize_workspace_schema().await);
        denied(runtime.delete_snapshot(SnapshotId::new()).await);
        denied(runtime.reap_expired_leases().await);
        denied(runtime.reap_packed_reader_sessions().await);
        denied(runtime.gc_snapshot(1, 1).await);
        denied(runtime.finalize_layer_metadata_deletion(Vec::new()).await);
        assert_eq!(*backend.records.lock().await, before);
        assert!(backend.cas_checks.lock().await.is_empty());
        assert!(backend.cas_write_keys.lock().await.is_empty());
        assert_eq!(backend.scans.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.allocate_id("inode").await.unwrap(), 1);
        assert!(runtime.load_volume_header().await.unwrap().is_none());
        assert_eq!(Arc::as_ptr(&runtime), identity);
    }

    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use tokio::sync::{Barrier, Mutex};
    use uuid::Uuid;

    use super::*;
    use crate::workspace_overlay::catalog::{
        AcquireLease, AppendDataExtent, CreateVolumeRoot, CreateWorkspace, DentryQuery, HeadGuard,
        NamespaceMutation, WorkspaceStore,
    };
    use crate::workspace_overlay::ids::{LeaseId, SnapshotId};
    use crate::workspace_overlay::packed_v3::wire005::{
        V3_FOOTER_LEN, V3_HEADER_LEN, V3ObjectKind, V3ObjectRef,
    };
    use crate::workspace_overlay::stores::redis::RedisWorkspaceBackend;
    use crate::workspace_overlay::stores::tikv::TiKvWorkspaceBackend;

    #[derive(Clone, Default)]
    struct MemoryBackend {
        records: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
        cas_checks: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
        cas_write_keys: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
        scans: Arc<AtomicU64>,
        fail_cas_once: Arc<std::sync::atomic::AtomicBool>,
        fail_busy_once: Arc<std::sync::atomic::AtomicBool>,
        fail_bounded_read_busy_once: Arc<std::sync::atomic::AtomicBool>,
        clock: Arc<AtomicI64>,
        advance_on_cas_ns: Arc<AtomicI64>,
        mutate_on_cas: Arc<Mutex<Vec<KvWrite>>>,
        timed_reads_until_mutation: Arc<AtomicU64>,
        mutate_on_timed_read: Arc<Mutex<Vec<KvWrite>>>,
    }

    // Each simulated catalog/process owns one explicit test mount ledger.
    // Child scheduling wrappers reuse this constructor; production callers
    // pass their existing mount/admin Arc through the CLI integration.
    fn budgeted_kv_store<B: WorkspaceKvBackend>(backend: B) -> KvWorkspaceStore<B> {
        KvWorkspaceStore::new(backend).with_packed_reader_pin_budget(
            crate::workspace_overlay::packed_v3::wire005::V3MountBudget::defaults(),
        )
    }

    pub(crate) fn packed_adapter_test_store(
        budget: Arc<crate::workspace_overlay::packed_v3::wire005::V3MountBudget>,
    ) -> impl WorkspaceStore {
        KvWorkspaceStore::new(MemoryBackend::default()).with_packed_reader_pin_budget(budget)
    }

    impl MemoryBackend {
        fn clock_ns(&self) -> Result<i64, WorkspaceError> {
            let overridden = self.clock.load(Ordering::SeqCst);
            if overridden != 0 {
                return Ok(overridden);
            }
            let duration = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| WorkspaceError::Backend(error.to_string()))?;
            i64::try_from(duration.as_nanos())
                .map_err(|_| WorkspaceError::Backend("test clock overflow".into()))
        }

        async fn cas(
            &self,
            checks: &[KvCheck],
            writes: &[KvWrite],
            deadline: Option<i64>,
        ) -> Result<bool, WorkspaceError> {
            self.cas_with_authentication_limits(checks, writes, deadline, None)
                .await
        }

        async fn cas_with_authentication_limits(
            &self,
            checks: &[KvCheck],
            writes: &[KvWrite],
            deadline: Option<i64>,
            authentication_limits: Option<
                crate::workspace_overlay::stores::kv_backend::KvReadLimits,
            >,
        ) -> Result<bool, WorkspaceError> {
            self.cas_checks
                .lock()
                .await
                .push(checks.iter().map(|check| check.key.clone()).collect());
            self.cas_write_keys.lock().await.push(
                writes
                    .iter()
                    .map(|write| match write {
                        KvWrite::Put { key, .. } | KvWrite::Delete { key } => key.clone(),
                    })
                    .collect(),
            );
            let mut records = self.records.lock().await;
            for write in self.mutate_on_cas.lock().await.drain(..) {
                apply_memory_write(&mut records, write);
            }
            if let Some(limits) = authentication_limits {
                let mut authentication_total = 0usize;
                for check in checks {
                    let value_bytes = records.get(&check.key).map_or(0, Vec::len);
                    authentication_total = authentication_total
                        .checked_add(check.key.len())
                        .and_then(|bytes| bytes.checked_add(value_bytes))
                        .ok_or_else(|| {
                            WorkspaceError::InvalidReadPlan(
                                "fixture authentication byte count overflow".into(),
                            )
                        })?;
                    let response_bytes = check
                        .key
                        .len()
                        .checked_add(value_bytes)
                        .and_then(|bytes| bytes.checked_add(16))
                        .ok_or_else(|| {
                            WorkspaceError::InvalidReadPlan(
                                "fixture authentication response byte count overflow".into(),
                            )
                        })?;
                    if value_bytes > limits.max_value_bytes
                        || authentication_total > limits.max_total_bytes
                        || response_bytes > limits.max_response_bytes
                    {
                        return Err(WorkspaceError::InvalidReadPlan(
                            "fixture authentication snapshot exceeds byte limits".into(),
                        ));
                    }
                }
            }
            if checks
                .iter()
                .any(|check| records.get(&check.key) != check.expected.as_ref())
            {
                return Ok(false);
            }
            if self.fail_busy_once.swap(false, Ordering::SeqCst) {
                return Err(WorkspaceError::Busy);
            }
            let advance = self.advance_on_cas_ns.swap(0, Ordering::SeqCst);
            if advance != 0 {
                self.clock.fetch_add(advance, Ordering::SeqCst);
            }
            if let Some(expiry) = deadline
                && self.clock_ns()? >= expiry
            {
                return Err(WorkspaceError::Fenced);
            }
            if self.fail_cas_once.swap(false, Ordering::SeqCst) {
                return Err(WorkspaceError::Backend("injected precommit failure".into()));
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
    }

    #[async_trait]
    impl WorkspaceKvBackend for MemoryBackend {
        fn supports_consistent_reads(&self) -> bool {
            true
        }
        fn name(&self) -> &'static str {
            "workspace-memory-test"
        }

        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, WorkspaceError> {
            Ok(self.records.lock().await.get(key).cloned())
        }

        async fn get_many_consistent(
            &self,
            keys: &[Vec<u8>],
        ) -> Result<Vec<Option<Vec<u8>>>, WorkspaceError> {
            let records = self.records.lock().await;
            Ok(keys.iter().map(|key| records.get(key).cloned()).collect())
        }

        async fn get_many_consistent_with_time(
            &self,
            keys: &[Vec<u8>],
        ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
            let mut records = self.records.lock().await;
            let remaining = self.timed_reads_until_mutation.load(Ordering::SeqCst);
            if remaining > 0
                && self
                    .timed_reads_until_mutation
                    .fetch_sub(1, Ordering::SeqCst)
                    == 1
            {
                for write in self.mutate_on_timed_read.lock().await.drain(..) {
                    apply_memory_write(&mut records, write);
                }
            }
            let values = keys.iter().map(|key| records.get(key).cloned()).collect();
            let now = self.clock_ns()?;
            Ok((values, now))
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

        async fn get_many_consistent_with_time_bounded(
            &self,
            keys: &[Vec<u8>],
            limits: super::super::kv_backend::KvReadLimits,
        ) -> Result<(Vec<Option<Vec<u8>>>, i64), WorkspaceError> {
            limits.validate_keys(keys)?;
            if self
                .fail_bounded_read_busy_once
                .swap(false, Ordering::SeqCst)
            {
                return Err(WorkspaceError::Busy);
            }
            let mut records = self.records.lock().await;
            let remaining = self.timed_reads_until_mutation.load(Ordering::SeqCst);
            if remaining > 0
                && self
                    .timed_reads_until_mutation
                    .fetch_sub(1, Ordering::SeqCst)
                    == 1
            {
                for write in self.mutate_on_timed_read.lock().await.drain(..) {
                    apply_memory_write(&mut records, write);
                }
            }
            let mut total = 0usize;
            for key in keys {
                let bytes = records.get(key).map_or(0, Vec::len);
                total += key.len() + bytes;
                if bytes > limits.max_value_bytes || total > limits.max_total_bytes {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "bounded test point value bytes exceeded".into(),
                    ));
                }
            }
            let values = keys.iter().map(|key| records.get(key).cloned()).collect();
            Ok((values, self.clock_ns()?))
        }

        async fn scan_prefix_with_byte_limits(
            &self,
            prefix: &[u8],
            limits: super::super::kv_backend::KvReadLimits,
        ) -> Result<Vec<KvEntry>, WorkspaceError> {
            limits.validate()?;
            let records = self.records.lock().await;
            let selected = || {
                records
                    .range(prefix.to_vec()..)
                    .take_while(|(key, _)| key.starts_with(prefix))
                    .take(limits.max_records)
            };
            let mut total = 0usize;
            for (key, value) in selected() {
                total += key.len() + value.len();
                if key.len() > limits.max_key_bytes
                    || value.len() > limits.max_value_bytes
                    || total > limits.max_total_bytes
                {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "bounded test scan value bytes exceeded".into(),
                    ));
                }
            }
            self.scans.fetch_add(1, Ordering::Relaxed);
            Ok(selected()
                .map(|(key, value)| KvEntry {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect())
        }

        async fn scan_prefix_bounded(
            &self,
            prefix: &[u8],
            max_records: usize,
        ) -> Result<Vec<KvEntry>, WorkspaceError> {
            if max_records == 0 {
                return Err(WorkspaceError::InvalidReadPlan(
                    "bounded prefix scan requires a positive row limit".into(),
                ));
            }
            self.scans.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .records
                .lock()
                .await
                .range(prefix.to_vec()..)
                .take_while(|(key, _)| key.starts_with(prefix))
                .take(max_records)
                .map(|(key, value)| KvEntry {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect())
        }

        async fn scan_prefix_page_with_byte_limits(
            &self,
            prefix: &[u8],
            after: Option<&[u8]>,
            limits: super::super::kv_backend::KvReadLimits,
        ) -> Result<Vec<KvEntry>, WorkspaceError> {
            limits.validate_scan_page(prefix, after)?;
            let records = self.records.lock().await;
            let selected = || {
                records
                    .range(prefix.to_vec()..)
                    .take_while(|(key, _)| key.starts_with(prefix))
                    .filter(|(key, _)| after.is_none_or(|after| key.as_slice() > after))
                    .take(limits.max_records.min(limits.max_data_requests))
            };
            let mut total = 0usize;
            let mut response = 16usize;
            for (key, value) in selected() {
                total = total
                    .checked_add(key.len())
                    .and_then(|sum| sum.checked_add(value.len()))
                    .ok_or(WorkspaceError::Busy)?;
                response = response
                    .checked_add(32)
                    .and_then(|sum| sum.checked_add(key.len()))
                    .and_then(|sum| sum.checked_add(value.len()))
                    .ok_or(WorkspaceError::Busy)?;
                if key.len() > limits.max_key_bytes
                    || value.len() > limits.max_value_bytes
                    || total > limits.max_total_bytes
                    || response > limits.max_response_bytes
                {
                    return Err(WorkspaceError::InvalidReadPlan(
                        "bounded test keyset page exceeds limits before cloning".into(),
                    ));
                }
            }
            self.scans.fetch_add(1, Ordering::Relaxed);
            Ok(selected()
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
            self.cas(checks, writes, None).await
        }

        async fn compare_and_swap_before(
            &self,
            checks: &[KvCheck],
            writes: &[KvWrite],
            expires_at_ns: i64,
        ) -> Result<bool, WorkspaceError> {
            self.cas(checks, writes, Some(expires_at_ns)).await
        }

        async fn authenticate_checks_before_bounded(
            &self,
            checks: &[KvCheck],
            expires_at_ns: i64,
            limits: crate::workspace_overlay::stores::kv_backend::KvReadLimits,
        ) -> Result<bool, WorkspaceError> {
            crate::workspace_overlay::stores::kv_backend::validate_bounded_authentication_checks(
                checks, limits,
            )?;
            crate::workspace_overlay::stores::kv_backend::validate_cas_time_window(
                None,
                Some(expires_at_ns),
            )?;
            self.cas_with_authentication_limits(checks, &[], Some(expires_at_ns), Some(limits))
                .await
        }

        async fn server_time_ns(&self) -> Result<i64, WorkspaceError> {
            self.clock_ns()
        }
    }

    fn apply_memory_write(records: &mut BTreeMap<Vec<u8>, Vec<u8>>, write: KvWrite) {
        match write {
            KvWrite::Put { key, value } => {
                records.insert(key, value);
            }
            KvWrite::Delete { key } => {
                records.remove(&key);
            }
        }
    }

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn create_request(offset: u128) -> CreateVolumeRoot {
        CreateVolumeRoot {
            volume_format: "workspace-v1".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
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
        let store = budgeted_kv_store(MemoryBackend::default());
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
        let workspace = store.load_workspace(workspace.workspace_id).await.unwrap();
        (store, workspace, lease, guard)
    }

    // Catalog-only identity fixture; it does not prove uploaded object closure.
    async fn install_open_binding_fixture(
        store: &KvWorkspaceStore<MemoryBackend>,
        workspace: &mut WorkspaceRecord,
        version: u64,
    ) -> PackedLowerBindingRecord {
        let workspace_key = hot_workspace_key(workspace.workspace_id);
        let workspace_raw = store.backend.get(&workspace_key).await.unwrap();
        let head: LayerRecord = decode(
            &store
                .backend
                .get(&hot_layer_key(workspace.head_layer_id))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let base: LayerRecord = decode(
            &store
                .backend
                .get(&hot_layer_key(head.parent_layer_id.unwrap()))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        workspace.head_epoch = 1;
        let mut binding = PackedLowerBindingRecord {
            workspace_id: workspace.workspace_id,
            head_layer_id: workspace.head_layer_id,
            head_epoch: workspace.head_epoch,
            base_revision: revision_from_layer(&base).unwrap(),
            highest_inode: 1,
            binding: PackedLowerBinding {
                binding_version: 1,
                base_layer_id: base.layer_id,
                manifest: V3ObjectRef {
                    key: "objects/open-topology-manifest".into(),
                    kind: V3ObjectKind::Manifest,
                    object_len: (V3_HEADER_LEN + V3_FOOTER_LEN) as u64,
                    digest: [7; 32],
                },
            },
        };
        let first = binding.encode().unwrap();
        binding.binding.binding_version = version;
        let bytes = binding.encode().unwrap();
        let mut checks = vec![KvCheck {
            key: workspace_key.clone(),
            expected: workspace_raw,
        }];
        let mut writes = vec![
            KvWrite::Put {
                key: workspace_key,
                value: encode(workspace).unwrap(),
            },
            KvWrite::Put {
                key: packed_claim_key(workspace.workspace_id),
                value: PACKED_CLAIM.to_vec(),
            },
            KvWrite::Put {
                key: packed_current_key(workspace.workspace_id),
                value: bytes.clone(),
            },
            KvWrite::Put {
                key: packed_history_key(workspace.workspace_id, 1),
                value: first,
            },
            KvWrite::Put {
                key: packed_history_key(workspace.workspace_id, version),
                value: bytes,
            },
        ];
        // This remains a catalog fixture, while the workspace epoch and its
        // canonical retention projection follow the actual owner CAS contract.
        let owner = store
            .prepare_native_owner_cas(&mut checks, &mut writes)
            .await
            .unwrap();
        assert!(
            store
                .backend
                .compare_and_swap(&checks, &writes)
                .await
                .unwrap()
        );
        drop(owner);
        binding
    }

    #[tokio::test]
    async fn kv_v3_open_topology_rejects_invalid_hot_base_for_open_and_ready() {
        for ready in [false, true] {
            for case in 0..13 {
                let (store, workspace, _lease, _guard) = initialized().await;
                let token = store
                    .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                    .await
                    .unwrap();
                let sidecar_key = open_v3_key(workspace.workspace_id);
                let before = store.backend.get(&sidecar_key).await.unwrap();
                let base_key = hot_layer_key(workspace.fork_base.as_ref().unwrap().layer_id);
                let mut base: LayerRecord =
                    decode(&store.backend.get(&base_key).await.unwrap().unwrap()).unwrap();
                match case {
                    0 => {}
                    1 => base.schema_version += 1,
                    2 => base.state = LayerState::Writable,
                    3 => base.depth = 2,
                    4 => base.parent_layer_id = Some(LayerId::new()),
                    5 => base.owner_workspace_id = Some(workspace.workspace_id),
                    6 => base.sealed_version = None,
                    7 => base.sealed_version = Some(0),
                    8 => base.delta_digest = None,
                    9 => base.root_hash = None,
                    10 => base.layer_id = LayerId::new(),
                    11 => base.sealed_at_ns = None,
                    _ => base.next_sequence = 0,
                }
                if case == 0 {
                    store.backend.records.lock().await.remove(&base_key);
                } else {
                    store
                        .backend
                        .records
                        .lock()
                        .await
                        .insert(base_key, encode(&base).unwrap());
                }
                let scans_before = store.backend.scans.load(Ordering::Relaxed);
                let result = if ready {
                    store.mark_workspace_v3_ready(&token).await
                } else {
                    store
                        .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                        .await
                };
                assert!(result.is_err(), "ready={ready}, base corruption={case}");
                assert_eq!(store.backend.get(&sidecar_key).await.unwrap(), before);
                assert_eq!(store.backend.scans.load(Ordering::Relaxed), scans_before);
            }
        }
    }

    #[tokio::test]
    async fn kv_v3_open_topology_rejects_invalid_head_and_workspace_state() {
        for case in 0..12 {
            let (store, mut workspace, _lease, _guard) = initialized().await;
            let head_key = hot_layer_key(workspace.head_layer_id);
            let mut head: LayerRecord =
                decode(&store.backend.get(&head_key).await.unwrap().unwrap()).unwrap();
            match case {
                0 => head.parent_layer_id = None,
                1 => head.parent_layer_id = Some(head.layer_id),
                2 => head.depth = 3,
                3 => head.sealed_version = Some(1),
                4 => head.delta_digest = Some([1; 32]),
                5 => head.root_hash = Some([1; 32]),
                6 => head.sealed_at_ns = Some(1),
                7 => head.next_sequence = 0,
                8 => head.state = LayerState::Sealing,
                9 => workspace.state = WorkspaceState::Sealing,
                10 => {
                    workspace.state = WorkspaceState::Sealing;
                    head.state = LayerState::Sealing;
                }
                _ => head.owner_workspace_id = Some(WorkspaceId::new()),
            }
            {
                let mut records = store.backend.records.lock().await;
                records.insert(head_key, encode(&head).unwrap());
                records.insert(
                    hot_workspace_key(workspace.workspace_id),
                    encode(&workspace).unwrap(),
                );
            }
            assert!(
                store
                    .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                    .await
                    .is_err(),
                "head/state corruption={case}"
            );
            assert!(
                store
                    .backend
                    .get(&open_v3_key(workspace.workspace_id))
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn kv_v3_open_topology_rejects_incomplete_or_stale_packed_binding() {
        for ready in [false, true] {
            for case in 0..17 {
                let (store, mut workspace, _lease, _guard) = initialized().await;
                let mut binding = install_open_binding_fixture(&store, &mut workspace, 1).await;
                let token = store
                    .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                    .await
                    .unwrap();
                let sidecar_key = open_v3_key(workspace.workspace_id);
                let before = store.backend.get(&sidecar_key).await.unwrap();
                let current_key = packed_current_key(workspace.workspace_id);
                let claim_key = packed_claim_key(workspace.workspace_id);
                let history_key = packed_history_key(workspace.workspace_id, 1);
                let mut records = store.backend.records.lock().await;
                match case {
                    0 => {
                        records.insert(current_key.clone(), b"bad".to_vec());
                    }
                    1 => {
                        records.remove(&claim_key);
                    }
                    2 => {
                        records.insert(claim_key, b"bad".to_vec());
                    }
                    3 => {
                        records.remove(&history_key);
                    }
                    4 => {
                        records.insert(history_key.clone(), b"bad".to_vec());
                    }
                    5 => {
                        records.remove(&current_key);
                    }
                    6 => {
                        records.remove(&current_key);
                        records.remove(&claim_key);
                    }
                    7 => binding.workspace_id = WorkspaceId::new(),
                    8 => binding.head_layer_id = LayerId::new(),
                    9 => binding.head_epoch += 1,
                    10 => binding.base_revision.sealed_version += 1,
                    11 => binding.base_revision.root_hash = [9; 32],
                    12 => {
                        binding.base_revision.layer_id = LayerId::new();
                        binding.binding.base_layer_id = binding.base_revision.layer_id;
                    }
                    13 => {
                        records.insert(current_key.clone(), vec![0; 8193]);
                        records.insert(history_key.clone(), vec![0; 8193]);
                    }
                    14 => {
                        records.remove(&current_key);
                        records.remove(&history_key);
                    }
                    15 => binding.binding.binding_version = 2,
                    _ => {
                        let mut head: LayerRecord = decode(
                            records
                                .get(&hot_layer_key(workspace.head_layer_id))
                                .unwrap(),
                        )
                        .unwrap();
                        head.depth = 3;
                        records.insert(hot_layer_key(head.layer_id), encode(&head).unwrap());
                    }
                }
                if (7..=12).contains(&case) || case == 15 {
                    let bytes = binding.encode().unwrap();
                    records.insert(current_key, bytes.clone());
                    records.insert(history_key, bytes);
                }
                drop(records);
                let result = if ready {
                    store.mark_workspace_v3_ready(&token).await
                } else {
                    store
                        .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                        .await
                };
                assert!(result.is_err(), "ready={ready}, binding corruption={case}");
                assert_eq!(store.backend.get(&sidecar_key).await.unwrap(), before);
            }
        }
    }

    #[tokio::test]
    async fn kv_binding_load_accepts_current_version_two_without_scans() {
        let (store, mut workspace, _lease, mut guard) = initialized().await;
        let binding = install_open_binding_fixture(&store, &mut workspace, 2).await;
        guard.expected_head_epoch = workspace.head_epoch;
        let scans_before = store.backend.scans.load(Ordering::Relaxed);
        assert_eq!(
            store.load_packed_binding_record(guard).await.unwrap(),
            Some(binding)
        );
        assert_eq!(store.backend.scans.load(Ordering::Relaxed), scans_before);
    }

    #[tokio::test]
    async fn kv_binding_load_rejects_base_key_value_identity_substitution() {
        let (store, mut workspace, _lease, mut guard) = initialized().await;
        let mut binding = install_open_binding_fixture(&store, &mut workspace, 2).await;
        guard.expected_head_epoch = workspace.head_epoch;
        let base_key = hot_layer_key(binding.base_revision.layer_id);
        let mut base: LayerRecord =
            decode(&store.backend.get(&base_key).await.unwrap().unwrap()).unwrap();
        base.layer_id = LayerId::new();
        binding.base_revision.layer_id = base.layer_id;
        binding.binding.base_layer_id = base.layer_id;
        let bytes = binding.encode().unwrap();
        let mut records = store.backend.records.lock().await;
        records.insert(base_key, encode(&base).unwrap());
        records.insert(packed_current_key(workspace.workspace_id), bytes.clone());
        records.insert(packed_history_key(workspace.workspace_id, 2), bytes);
        drop(records);
        assert!(matches!(
            store.load_packed_binding_record(guard).await,
            Err(WorkspaceError::Fenced | WorkspaceError::CorruptMetadata(_))
        ));
    }

    #[tokio::test]
    async fn kv_binding_load_rejects_missing_or_invalid_initial_history() {
        for case in 0..4 {
            let (store, mut workspace, _lease, mut guard) = initialized().await;
            let mut binding = install_open_binding_fixture(&store, &mut workspace, 2).await;
            guard.expected_head_epoch = workspace.head_epoch;
            let key = packed_history_key(workspace.workspace_id, 1);
            let mut records = store.backend.records.lock().await;
            match case {
                0 => {
                    records.remove(&key);
                }
                1 => {
                    records.insert(key, b"bad".to_vec());
                }
                2 => {
                    records.insert(key, binding.encode().unwrap());
                }
                _ => {
                    binding.binding.binding_version = 1;
                    binding.workspace_id = WorkspaceId::new();
                    records.insert(key, binding.encode().unwrap());
                }
            }
            drop(records);
            assert!(matches!(
                store.load_packed_binding_record(guard).await,
                Err(WorkspaceError::CorruptMetadata(_))
            ));
        }
    }

    #[tokio::test]
    async fn kv_binding_load_rejects_current_change_between_routing_and_final_read() {
        let (store, mut workspace, _lease, mut guard) = initialized().await;
        let mut binding = install_open_binding_fixture(&store, &mut workspace, 2).await;
        guard.expected_head_epoch = workspace.head_epoch;
        binding.binding.binding_version = 3;
        let bytes = binding.encode().unwrap();
        *store.backend.mutate_on_timed_read.lock().await = vec![
            KvWrite::Put {
                key: packed_current_key(workspace.workspace_id),
                value: bytes.clone(),
            },
            KvWrite::Put {
                key: packed_history_key(workspace.workspace_id, 3),
                value: bytes,
            },
        ];
        store
            .backend
            .timed_reads_until_mutation
            .store(2, Ordering::SeqCst);
        assert!(matches!(
            store.load_packed_binding_record(guard).await,
            Err(WorkspaceError::Fenced)
        ));
    }

    #[tokio::test]
    async fn kv_v3_open_topology_accepts_current_binding_version_two_without_scans() {
        let (store, mut workspace, _lease, _guard) = initialized().await;
        let binding = install_open_binding_fixture(&store, &mut workspace, 2).await;
        let scans_before = store.backend.scans.load(Ordering::Relaxed);
        let token = store
            .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
            .await
            .unwrap();
        store.mark_workspace_v3_ready(&token).await.unwrap();
        assert_eq!(store.backend.scans.load(Ordering::Relaxed), scans_before);
        let checks = store.backend.cas_checks.lock().await;
        for keys in checks.iter().rev().take(2) {
            for key in [
                hot_layer_key(binding.base_revision.layer_id),
                packed_current_key(workspace.workspace_id),
                packed_claim_key(workspace.workspace_id),
                packed_history_key(workspace.workspace_id, 1),
                packed_history_key(workspace.workspace_id, 2),
            ] {
                assert!(
                    keys.contains(&key),
                    "missing authority {}",
                    String::from_utf8_lossy(&key)
                );
            }
        }
    }

    #[tokio::test]
    async fn kv_v3_open_topology_rechecks_authorities_changed_at_cas() {
        for ready in [false, true] {
            for case in 0..4 {
                let (store, mut workspace, _lease, _guard) = initialized().await;
                let binding = install_open_binding_fixture(&store, &mut workspace, 1).await;
                let token = store
                    .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                    .await
                    .unwrap();
                let sidecar_key = open_v3_key(workspace.workspace_id);
                let before = store.backend.get(&sidecar_key).await.unwrap();
                let key = match case {
                    0 => hot_layer_key(binding.base_revision.layer_id),
                    1 => packed_current_key(workspace.workspace_id),
                    2 => packed_claim_key(workspace.workspace_id),
                    _ => packed_history_key(workspace.workspace_id, 1),
                };
                store
                    .backend
                    .mutate_on_cas
                    .lock()
                    .await
                    .push(KvWrite::Delete { key });
                let result = if ready {
                    store.mark_workspace_v3_ready(&token).await
                } else {
                    store
                        .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                        .await
                };
                assert!(result.is_err(), "ready={ready}, changed authority={case}");
                assert_eq!(store.backend.get(&sidecar_key).await.unwrap(), before);
            }
        }
    }

    #[tokio::test]
    async fn kv_v3_open_topology_rejects_binding_change_between_routing_and_final_read() {
        let (store, mut workspace, _lease, _guard) = initialized().await;
        let mut binding = install_open_binding_fixture(&store, &mut workspace, 1).await;
        binding.binding.binding_version = 2;
        let bytes = binding.encode().unwrap();
        *store.backend.mutate_on_timed_read.lock().await = vec![
            KvWrite::Put {
                key: packed_current_key(workspace.workspace_id),
                value: bytes.clone(),
            },
            KvWrite::Put {
                key: packed_history_key(workspace.workspace_id, 2),
                value: bytes,
            },
        ];
        store
            .backend
            .timed_reads_until_mutation
            .store(2, Ordering::SeqCst);
        assert!(matches!(
            store
                .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                .await,
            Err(WorkspaceError::Busy)
        ));
        assert!(
            store
                .backend
                .get(&open_v3_key(workspace.workspace_id))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn kv_v3_open_sidecar_fences_live_owner_and_increments_takeover_generation() {
        let (store, workspace, _lease, _guard) = initialized().await;
        let scans_before = store.backend.scans.load(Ordering::Relaxed);
        let first = store
            .open_workspace_v3(workspace.workspace_id, "sidecar-a", Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(store.backend.scans.load(Ordering::Relaxed), scans_before);
        assert_eq!(first.generation, 1);
        assert_eq!(first.state, V3OpenState::Ready);
        assert!(!first.recovery_required);

        let same_owner = store
            .open_workspace_v3(workspace.workspace_id, "sidecar-a", Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(same_owner.generation, first.generation);
        assert!(matches!(
            store
                .open_workspace_v3(workspace.workspace_id, "sidecar-b", Duration::from_secs(30),)
                .await,
            Err(WorkspaceError::Busy)
        ));

        store.close_workspace_v3(&first).await.unwrap();
        let takeover = store
            .open_workspace_v3(workspace.workspace_id, "sidecar-b", Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(takeover.generation, 2);
        assert!(matches!(
            store.close_workspace_v3(&first).await,
            Err(WorkspaceError::Fenced)
        ));
        store.close_workspace_v3(&takeover).await.unwrap();
    }

    #[tokio::test]
    async fn kv_v3_open_sidecar_expiry_during_cas_never_writes_or_revives_an_owner() {
        for operation in 0..5 {
            let (store, workspace, _lease, _guard) = initialized().await;
            store.backend.clock.store(1_000_000_000, Ordering::SeqCst);
            let key = open_v3_key(workspace.workspace_id);
            if operation == 4 {
                store
                    .backend
                    .advance_on_cas_ns
                    .store(30_000_000_000, Ordering::SeqCst);
                assert!(matches!(
                    store
                        .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                        .await,
                    Err(WorkspaceError::Fenced)
                ));
                assert!(store.backend.get(&key).await.unwrap().is_none());
                continue;
            }
            let token = store
                .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                .await
                .unwrap();
            let before = store.backend.get(&key).await.unwrap();
            store
                .backend
                .advance_on_cas_ns
                .store(30_000_000_000, Ordering::SeqCst);
            let result = match operation {
                0 => store
                    .renew_workspace_v3(&token, Duration::from_secs(30))
                    .await
                    .map(|_| ()),
                1 => store.mark_workspace_v3_ready(&token).await.map(|_| ()),
                2 => store.close_workspace_v3(&token).await,
                _ => store
                    .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                    .await
                    .map(|_| ()),
            };
            assert!(
                matches!(result, Err(WorkspaceError::Fenced)),
                "operation {operation}"
            );
            assert_eq!(
                store.backend.get(&key).await.unwrap(),
                before,
                "operation {operation}"
            );
        }
    }

    #[tokio::test]
    async fn kv_v3_open_sidecar_rejects_oversized_owner_and_malformed_header_without_writes() {
        let (store, workspace, _lease, _guard) = initialized().await;
        let key = open_v3_key(workspace.workspace_id);
        assert!(matches!(
            store
                .open_workspace_v3(
                    workspace.workspace_id,
                    "x".repeat(257),
                    Duration::from_secs(30)
                )
                .await,
            Err(WorkspaceError::CorruptMetadata(_))
        ));
        let original = store.backend.get(VOLUME_HEADER_KEY).await.unwrap().unwrap();
        store.backend.records.lock().await.insert(
            VOLUME_HEADER_KEY.to_vec(),
            [original.as_slice(), &[0]].concat(),
        );
        assert!(matches!(
            store
                .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                .await,
            Err(WorkspaceError::CorruptMetadata(_))
        ));
        assert!(store.backend.get(&key).await.unwrap().is_none());

        store.backend.records.lock().await.insert(
            VOLUME_HEADER_KEY.to_vec(),
            vec![0; OPEN_V3_MAX_VALUE_BYTES + 1],
        );
        assert!(matches!(
            store
                .open_workspace_v3(workspace.workspace_id, "owner", Duration::from_secs(30))
                .await,
            Err(WorkspaceError::InvalidReadPlan(_))
        ));
        assert!(store.backend.get(&key).await.unwrap().is_none());
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
    async fn delta_reads_use_bounded_keyset_pages() {
        let (store, workspace, _lease, _guard) = initialized().await;
        {
            let mut records = store.backend.records.lock().await;
            for index in 0..(DELTA_SCAN_PAGE_RECORDS + 8) {
                let row = XattrDelta {
                    layer_id: workspace.head_layer_id,
                    ino: 2,
                    name: format!("user.page-{index:02}").into_bytes(),
                    op: ValueOp::Put,
                    value: Some(vec![index as u8]),
                    sequence: index as u64,
                };
                records.insert(xattr_key(&row), encode(&row).unwrap());
            }
        }
        store.backend.scans.store(0, Ordering::Relaxed);
        let rows = store
            .get_xattr_deltas(XattrQuery {
                layer_ids: vec![workspace.head_layer_id],
                ino: 2,
                name: None,
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), DELTA_SCAN_PAGE_RECORDS + 8);
        assert!(
            store.backend.scans.load(Ordering::Relaxed) >= 2,
            "the result must cross a bounded page boundary"
        );
    }

    #[tokio::test]
    async fn catalog_scans_use_bounded_keyset_pages() {
        let (store, workspace, _lease, _guard) = initialized().await;
        {
            let mut records = store.backend.records.lock().await;
            for index in 0..(GENERIC_SCAN_PAGE_RECORDS + 3) {
                let mut row = workspace.clone();
                row.workspace_id = WorkspaceId::from_uuid(id(10_000 + index as u128));
                row.created_at_ns = index as i64;
                records.insert(workspace_key(row.workspace_id), encode(&row).unwrap());
            }
        }
        store.backend.scans.store(0, Ordering::Relaxed);
        let rows = store.list_workspaces().await.unwrap();
        assert_eq!(rows.len(), GENERIC_SCAN_PAGE_RECORDS + 4);
        assert!(
            store.backend.scans.load(Ordering::Relaxed) >= 2,
            "the catalog must cross a bounded page boundary"
        );
    }

    #[test]
    fn generic_scan_page_validation_rejects_oversized_values() {
        let limits = generic_scan_limits();
        let page = vec![KvEntry {
            key: b"catalog/one".to_vec(),
            value: vec![0; limits.max_value_bytes + 1],
        }];
        assert!(matches!(
            validate_generic_scan_page(&page, b"catalog/", None),
            Err(WorkspaceError::InvalidReadPlan(_))
        ));
    }

    #[tokio::test]
    async fn bounded_extent_query_rejects_corrupt_rows_before_range_filtering() {
        for case in 0..8 {
            for range_start in [0, 32] {
                let (store, workspace, _lease, _guard) = initialized().await;
                let layers = store
                    .load_layer_chain(workspace.head_layer_id)
                    .await
                    .unwrap();
                let mut row = DataExtentDelta::data(workspace.head_layer_id, 2, 0, 0, 4, 100, 0, 1);
                let mut key = extent_key(&row);
                match case {
                    0 => row.layer_id = layers[1].layer_id,
                    1 => row.ino = 3,
                    2 => row.chunk_index = 1,
                    3 => row.sequence = 2,
                    4 => row.length = 0,
                    5 => row.logical_offset = u64::MAX,
                    6 => {
                        row.kind = ExtentKind::Data {
                            slice_id: 100,
                            slice_offset: u64::MAX,
                        }
                    }
                    7 => key.extend_from_slice(b"/extra"),
                    _ => unreachable!(),
                }
                store
                    .backend
                    .records
                    .lock()
                    .await
                    .insert(key, encode(&row).unwrap());
                let result = store
                    .get_extent_deltas_bounded(
                        ExtentQuery {
                            layer_ids: layers.iter().map(|layer| layer.layer_id).collect(),
                            ino: 2,
                            chunk_index: 0,
                            range_start,
                            range_end: range_start + 4,
                        },
                        4,
                    )
                    .await;
                assert!(
                    matches!(result, Err(WorkspaceError::CorruptMetadata(_))),
                    "case {case}, range start {range_start}: {result:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn truncate_holes_do_not_exhaust_extent_read_budget() {
        let (store, workspace, _lease, guard) = initialized().await;
        for _ in 0..4097 {
            store
                .append_data_extent(AppendDataExtent {
                    guard: guard.clone(),
                    extent: DataExtentDelta::hole(workspace.head_layer_id, 2, 0, 1, 8191, 0),
                    chunk_size: 8192,
                })
                .await
                .unwrap();
        }
        // Reopen the catalog: compaction must be persisted, not a mount cache.
        let reopened = budgeted_kv_store(store.backend.as_ref().clone());
        let rows = reopened
            .get_extent_deltas(ExtentQuery {
                layer_ids: vec![workspace.head_layer_id],
                ino: 2,
                chunk_index: 0,
                range_start: 0,
                range_end: 8192,
            })
            .await
            .expect("obsolete truncate holes must not consume the read budget");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].logical_offset, 1);
        assert_eq!(rows[0].length, 8191);
        assert_eq!(rows[0].kind, ExtentKind::Hole);
    }

    #[tokio::test]
    async fn bounded_extent_query_enforces_row_limit_before_returning_rows() {
        let (store, workspace, _lease, guard) = initialized().await;
        for index in 0..3_u64 {
            store
                .append_data_extent(AppendDataExtent {
                    guard: guard.clone(),
                    extent: DataExtentDelta::data(
                        workspace.head_layer_id,
                        2,
                        0,
                        index * 4,
                        4,
                        100 + index,
                        0,
                        0,
                    ),
                    chunk_size: 64 * 1024 * 1024,
                })
                .await
                .unwrap();
        }
        let layers = store
            .load_layer_chain(workspace.head_layer_id)
            .await
            .unwrap();
        let request = ExtentQuery {
            layer_ids: layers.iter().map(|layer| layer.layer_id).collect(),
            ino: 2,
            chunk_index: 0,
            range_start: 0,
            range_end: 32,
        };
        let error = store
            .get_extent_deltas_bounded(request.clone(), 2)
            .await
            .unwrap_err();
        assert!(matches!(error, WorkspaceError::InvalidReadPlan(_)));

        // Only the first row overlaps. The out-of-range sentinel must still
        // reject overflow before filtering, rather than hiding further rows.
        let error = store
            .get_extent_deltas_bounded(
                ExtentQuery {
                    range_end: 1,
                    ..request.clone()
                },
                2,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, WorkspaceError::InvalidReadPlan(_)));

        let rows = store.get_extent_deltas_bounded(request, 3).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert!(
            rows.windows(2)
                .all(|window| window[0].sequence > window[1].sequence)
        );
    }

    #[tokio::test]
    async fn kv_read_fence_checks_authority_and_both_layer_versions_consistently() {
        let (store, workspace, _lease, guard) = initialized().await;
        let layers = store
            .load_layer_chain(workspace.head_layer_id)
            .await
            .unwrap();
        let expected: [LayerRecord; 2] = [layers[0].clone(), layers[1].clone()];
        store
            .validate_read_fence(guard, expected.clone())
            .await
            .unwrap();

        let mut stale = expected;
        stale[0].next_sequence += 1;
        let guard = HeadGuard {
            workspace_id: workspace.workspace_id,
            expected_head_layer_id: workspace.head_layer_id,
            expected_head_epoch: workspace.head_epoch,
            lease_id: _lease.lease_id,
            holder_generation: _lease.holder_generation,
        };
        let error = store.validate_read_fence(guard, stale).await.unwrap_err();
        assert!(matches!(error, WorkspaceError::Busy));
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
    async fn kv_catalog_round_trips_mutations() {
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
    }

    #[tokio::test]
    async fn every_topology_transition_rebuilds_after_presubmit_busy() {
        // One shared rule: a pre-submit Busy from TopologyTxn::commit is an
        // ordinary CAS conflict. Exercise several distinct transitions with
        // their own retry loops; none may surface the transient Busy.
        let (store, workspace, lease, _guard) = initialized().await;
        let busy = || store.backend.fail_busy_once.store(true, Ordering::SeqCst);
        // release_lease builds its packet directly (not via TopologyTxn) and
        // keeps its own semantics; release without injection.
        store
            .release_lease(ReleaseLease {
                lease_id: lease.lease_id,
                holder_generation: lease.holder_generation,
            })
            .await
            .unwrap();
        busy();
        store
            .mark_workspace_deleting(MarkDeleting {
                workspace_id: workspace.workspace_id,
                force_fence_lease: false,
            })
            .await
            .expect("mark_workspace_deleting rebuilds");
        assert!(!store.backend.fail_busy_once.load(Ordering::SeqCst));
        assert_eq!(
            store
                .load_workspace(workspace.workspace_id)
                .await
                .unwrap()
                .state,
            WorkspaceState::Deleting
        );
    }

    #[tokio::test]
    async fn acquire_lease_retries_busy_returned_by_topology_commit() {
        let (store, workspace, lease, _guard) = initialized().await;
        store
            .release_lease(ReleaseLease {
                lease_id: lease.lease_id,
                holder_generation: lease.holder_generation,
            })
            .await
            .unwrap();
        // A sibling seal/fork can advance the shared root epoch so packet
        // preparation returns Busy before the acquire packet is submitted.
        store.backend.fail_busy_once.store(true, Ordering::SeqCst);
        let acquired = store
            .acquire_lease(AcquireLease {
                workspace_id: workspace.workspace_id,
                lease_id: LeaseId::from_uuid(id(621)),
                holder_generation: lease.holder_generation + 1,
                ttl_ns: 120_000_000_000,
            })
            .await
            .expect("transient acquire Busy should be retried");
        assert!(!store.backend.fail_busy_once.load(Ordering::SeqCst));
        assert_eq!(acquired.state, LeaseState::Active);
        assert_eq!(
            store
                .load_workspace(workspace.workspace_id)
                .await
                .unwrap()
                .active_lease,
            Some(acquired.lease_id)
        );
    }

    #[tokio::test]
    async fn independent_kv_store_instances_use_backend_cas() {
        let backend = MemoryBackend::default();
        let probe = backend.clone();
        let store_a = Arc::new(budgeted_kv_store(backend.clone()));
        let store_b = Arc::new(budgeted_kv_store(backend));
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
        let expected_mutation_checks = [(&first, &lease_a, b'a'), (&second, &lease_b, b'b')]
            .into_iter()
            .flat_map(|(workspace, lease, tag)| {
                (0..32).map(move |index| {
                    BTreeSet::from([
                        hot_workspace_key(workspace.workspace_id),
                        hot_layer_key(workspace.head_layer_id),
                        hot_lease_key(lease.workspace_id, lease.lease_id),
                        packed_current_key(workspace.workspace_id),
                        packed_claim_key(workspace.workspace_id),
                        packed_history_key(workspace.workspace_id, 1),
                        packed_writer_authority::packed_writer_key(workspace.workspace_id),
                        native_reverse::state_key(workspace.head_layer_id),
                        dentry_identity_key(
                            workspace.head_layer_id,
                            1,
                            format!("{}-{index}", tag as char).as_bytes(),
                        ),
                    ])
                })
            })
            .collect::<BTreeSet<_>>();
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
            let actual = keys.iter().cloned().collect::<BTreeSet<_>>();
            keys.len() == 9
                && expected_mutation_checks
                    .iter()
                    .any(|expected| &actual == expected)
        }));
        for expected in expected_mutation_checks {
            assert_eq!(
                checks
                    .iter()
                    .filter(|keys| { keys.iter().cloned().collect::<BTreeSet<_>>() == expected })
                    .count(),
                1,
                "each actual native writer must authenticate its exact dentry and reverse state"
            );
        }
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
    async fn slice_ids_are_reserved_in_blocks_and_unique_across_stores() {
        let backend = MemoryBackend::default();
        let first = budgeted_kv_store(backend.clone());
        first.initialize_workspace_schema().await.unwrap();
        first.create_volume_root(create_request(210)).await.unwrap();
        let second = budgeted_kv_store(backend.clone());
        backend.cas_write_keys.lock().await.clear();
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..SLICE_ID_BLOCK {
            assert!(seen.insert(first.allocate_id("slice").await.unwrap()));
        }
        // One durable allocator CAS covers the whole block.
        assert_eq!(backend.cas_write_keys.lock().await.len(), 1);
        for _ in 0..3 {
            assert!(seen.insert(second.allocate_id("slice").await.unwrap()));
            assert!(seen.insert(first.allocate_id("slice").await.unwrap()));
        }
        // Inode IDs stay dense: no block reservation.
        let inode = first.allocate_id("inode").await.unwrap();
        assert_eq!(first.allocate_id("inode").await.unwrap(), inode + 1);
    }

    #[tokio::test]
    async fn lease_heartbeat_and_allocators_check_header_without_rewriting_it() {
        let backend = MemoryBackend::default();
        let store = budgeted_kv_store(backend.clone());
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
        backend.cas_write_keys.lock().await.clear();
        let control_before = backend.get(CONTROL_KEY).await.unwrap();
        let topology_before = backend.get(TOPOLOGY_GENERATION_KEY).await.unwrap();

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
                .all(|keys| keys.iter().all(|key| key.as_slice() != CONTROL_KEY))
        );
        let write_keys = backend.cas_write_keys.lock().await.clone();
        assert_eq!(write_keys.len(), 3);
        assert!(write_keys.iter().flatten().all(|key| {
            key.as_slice() != CONTROL_KEY && key.as_slice() != TOPOLOGY_GENERATION_KEY
        }));
        assert_eq!(backend.get(CONTROL_KEY).await.unwrap(), control_before);
        assert_eq!(
            backend.get(TOPOLOGY_GENERATION_KEY).await.unwrap(),
            topology_before
        );
        // Heartbeat also authenticates its hold's source layer in the same CAS;
        // allocators authenticate their own key. The immutable catalog header is
        // validated during packet preparation without entering the lock set.
        let expected_lease_checks = BTreeSet::from([
            hot_lease_key(lease.workspace_id, lease.lease_id),
            lease_index_key(lease.lease_id),
            b"packed/v3/native-hold-feature".to_vec(),
            PACKED_ROOT_GENERATION_KEY.to_vec(),
            LAYER_INVENTORY_GENERATION_KEY.to_vec(),
            format!("packed/v3/native-hold/lease/{}", lease.lease_id).into_bytes(),
            packed_current_key(workspace.workspace_id),
            packed_claim_key(workspace.workspace_id),
            packed_history_key(workspace.workspace_id, 1),
            packed_writer_authority::packed_writer_key(workspace.workspace_id),
            hot_layer_key(lease.base_revision.layer_id),
        ]);
        assert_eq!(checks[0].len(), 11);
        assert_eq!(
            checks[0].iter().cloned().collect::<BTreeSet<_>>(),
            expected_lease_checks
        );
        for (keys, kind) in checks[1..].iter().zip(["inode", "slice"]) {
            assert_eq!(keys.len(), 1);
            assert_eq!(
                keys.iter().cloned().collect::<BTreeSet<_>>(),
                BTreeSet::from([allocator_key(kind)])
            );
        }
    }

    async fn remote_backend_contract<B>(
        store_a: Arc<KvWorkspaceStore<B>>,
        store_b: Arc<KvWorkspaceStore<B>>,
    ) where
        B: WorkspaceKvBackend,
    {
        store_a.initialize_workspace_schema().await.unwrap();
        let request = CreateVolumeRoot {
            volume_format: "workspace-v1".into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
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
        permission_backend_contract(store_a, store_b, workspace, guard).await;
    }

    fn access_acl(mode: u32) -> Vec<u8> {
        let mut bytes = 2u32.to_le_bytes().to_vec();
        for (tag, permission, id) in [
            (1u16, ((mode >> 6) & 7) as u16, u32::MAX),
            (2, 7, 1234),
            (4, 7, u32::MAX),
            (16, ((mode >> 3) & 7) as u16, u32::MAX),
            (32, (mode & 7) as u16, u32::MAX),
        ] {
            bytes.extend_from_slice(&tag.to_le_bytes());
            bytes.extend_from_slice(&permission.to_le_bytes());
            bytes.extend_from_slice(&id.to_le_bytes());
        }
        bytes
    }

    fn permission_request(
        snapshot: &PermissionSnapshot,
        guard: &HeadGuard,
        mode: u32,
    ) -> PermissionMutation {
        let mut inode = crate::workspace_overlay::resolver::resolve_inode(
            &snapshot.layers,
            &snapshot.inodes,
            1,
        )
        .unwrap()
        .unwrap()
        .inode;
        inode.layer_id = guard.expected_head_layer_id;
        inode.mode = mode;
        inode.ctime_ns += 1;
        inode.sequence = 0;
        PermissionMutation {
            guard: guard.clone(),
            expected_layers: snapshot.layers.clone(),
            dentries: Vec::new(),
            inodes: vec![inode],
            xattrs: vec![XattrDelta {
                layer_id: guard.expected_head_layer_id,
                ino: 1,
                name: crate::meta::posix_acl::ACCESS_XATTR.to_vec(),
                op: ValueOp::Put,
                value: Some(access_acl(mode)),
                sequence: 0,
            }],
        }
    }

    fn permission_query(workspace: &WorkspaceRecord, base: LayerId) -> PermissionSnapshotQuery {
        PermissionSnapshotQuery {
            layer_ids: [workspace.head_layer_id, base],
            inodes: vec![1],
            dentry: None,
        }
    }

    async fn permission_backend_contract<B: WorkspaceKvBackend>(
        store_a: Arc<KvWorkspaceStore<B>>,
        store_b: Arc<KvWorkspaceStore<B>>,
        workspace: WorkspaceRecord,
        guard: HeadGuard,
    ) {
        use crate::meta::posix_acl::{ACCESS_XATTR, PosixAcl};
        let base = store_a
            .load_layer(workspace.head_layer_id)
            .await
            .unwrap()
            .parent_layer_id
            .unwrap();
        let query = permission_query(&workspace, base);
        let snapshot = store_a
            .read_permission_snapshot(query.clone())
            .await
            .unwrap();
        let stale = permission_request(&snapshot, &guard, 0o640);
        let mut stale_touch = VersionedMutation::empty(
            guard.clone(),
            snapshot.layers.clone(),
            crate::chunk::layout::DEFAULT_CHUNK_SIZE,
        );
        stale_touch.inodes = stale.inodes.clone();
        let barrier = Arc::new(Barrier::new(2));
        let writers = [(store_a.clone(), 0o640), (store_b.clone(), 0o604)]
            .into_iter()
            .map(|(store, mode)| {
                let barrier = barrier.clone();
                let request = permission_request(&snapshot, &guard, mode);
                tokio::spawn(async move {
                    barrier.wait().await;
                    store.apply_permission_mutation(request).await
                })
            })
            .collect::<Vec<_>>();
        let mut committed = 0;
        let mut conflicted = 0;
        for writer in writers {
            match writer.await.unwrap() {
                Ok(_) => committed += 1,
                Err(WorkspaceError::Busy) => conflicted += 1,
                Err(error) => panic!("unexpected conditional mutation error: {error}"),
            }
        }
        assert_eq!(
            (committed, conflicted),
            (1, 1),
            "stale permission policy was replayed"
        );
        assert!(
            matches!(
                store_b.apply_versioned_mutation(stale_touch).await,
                Err(WorkspaceError::Busy)
            ),
            "non-ACL inode writer overwrote a newer mode/ACL commit"
        );
        let snapshot = store_b
            .read_permission_snapshot(query.clone())
            .await
            .unwrap();
        let inode = crate::workspace_overlay::resolver::resolve_inode(
            &snapshot.layers,
            &snapshot.inodes,
            1,
        )
        .unwrap()
        .unwrap()
        .inode;
        let acl = crate::workspace_overlay::resolver::resolve_xattr(
            &snapshot.layers,
            &snapshot.xattrs,
            1,
            ACCESS_XATTR,
        )
        .unwrap()
        .unwrap()
        .value;
        assert_eq!(
            PosixAcl::decode(&acl).unwrap().mode_bits(),
            inode.mode & 0o777
        );

        // Independent writer and reader instances repeatedly race. Every
        // snapshot must contain a complete mode+ACL version without EIO/fallback.
        let writer_query = query.clone();
        let writer_guard = guard.clone();
        let writer_store = store_a.clone();
        let writer = tokio::spawn(async move {
            for index in 0..64 {
                let snapshot = writer_store
                    .read_permission_snapshot(writer_query.clone())
                    .await
                    .unwrap();
                let mode = if index % 2 == 0 { 0o640 } else { 0o604 };
                writer_store
                    .apply_permission_mutation(permission_request(&snapshot, &writer_guard, mode))
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        });
        for _ in 0..128 {
            let snapshot = store_b
                .read_permission_snapshot(query.clone())
                .await
                .unwrap();
            let inode = crate::workspace_overlay::resolver::resolve_inode(
                &snapshot.layers,
                &snapshot.inodes,
                1,
            )
            .unwrap()
            .unwrap()
            .inode;
            let acl = crate::workspace_overlay::resolver::resolve_xattr(
                &snapshot.layers,
                &snapshot.xattrs,
                1,
                ACCESS_XATTR,
            )
            .unwrap()
            .unwrap()
            .value;
            assert_eq!(
                PosixAcl::decode(&acl).unwrap().mode_bits(),
                inode.mode & 0o777
            );
            tokio::task::yield_now().await;
        }
        writer.await.unwrap();
        writable_meta_backend_contract(store_a, store_b, guard).await;
    }

    async fn writable_meta_backend_contract<B: WorkspaceKvBackend>(
        a: Arc<KvWorkspaceStore<B>>,
        b: Arc<KvWorkspaceStore<B>>,
        guard: HeadGuard,
    ) {
        use crate::meta::layer::MetaLayer;
        use crate::meta::store::{FileType, SetAttrFlags, SetAttrRequest};
        use crate::workspace_overlay::meta_layer::WorkspaceMetaLayer;
        let view = ViewContext {
            workspace_id: guard.workspace_id,
            head_layer_id: guard.expected_head_layer_id,
            head_epoch: guard.expected_head_epoch,
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
        };
        let meta_a = WorkspaceMetaLayer::new(a.clone(), view.clone());
        let meta_b = WorkspaceMetaLayer::new(b.clone(), view);
        meta_a
            .update_posix_acl(
                1,
                "system.posix_acl_default",
                Some(&access_acl(0o770)),
                0,
                &[],
            )
            .await
            .unwrap();
        let parent = meta_a
            .create_node_with_umask(
                1,
                "acl-contract-parent".into(),
                FileType::Dir,
                0o777,
                0o077,
                1000,
                2000,
                0,
            )
            .await
            .unwrap()
            .ino;
        let child = meta_b
            .create_node_with_umask(
                parent,
                "child".into(),
                FileType::File,
                0o666,
                0o077,
                1000,
                2000,
                0,
            )
            .await
            .unwrap()
            .ino;
        let permissions = meta_a.inode_permissions(child).await.unwrap().unwrap();
        assert_eq!(permissions.attr.mode, 0o660);
        assert_eq!(permissions.access_acl, Some(access_acl(0o660)));
        meta_b
            .set_xattr(child, "user.permission-marker", b"retained", 0)
            .await
            .unwrap();
        meta_a
            .set_attr(
                child,
                &SetAttrRequest {
                    mode: Some(0o751),
                    size: Some(8192),
                    ..Default::default()
                },
                SetAttrFlags::empty(),
            )
            .await
            .unwrap();
        meta_b.link(child, parent, "alias").await.unwrap();
        meta_a
            .rename(parent, "child", parent, "moved".into())
            .await
            .unwrap();
        assert_eq!(
            meta_b
                .get_xattr(child, "user.permission-marker")
                .await
                .unwrap(),
            Some(b"retained".to_vec())
        );
        let permissions = meta_b.inode_permissions(child).await.unwrap().unwrap();
        assert_eq!(
            (
                permissions.attr.mode,
                permissions.attr.size,
                permissions.attr.nlink
            ),
            (0o751, 8192, 2)
        );
        assert_eq!(permissions.access_acl, Some(access_acl(0o751)));
        meta_a
            .remove_xattr(child, "system.posix_acl_access")
            .await
            .unwrap();
        assert_eq!(
            meta_b
                .inode_permissions(child)
                .await
                .unwrap()
                .unwrap()
                .attr
                .mode,
            0o751
        );
        // Independent clients must apply fresh control ACL policy to both
        // truncate and exact NOW semantics, not merely preserve mode/ACL rows.
        let denied_control = serde_json::to_vec(&vec![
            crate::control::protocol::ControlAclEntry {
                scope: "access".into(),
                tag: "user_obj".into(),
                id: None,
                perm: "rwx".into(),
            },
            crate::control::protocol::ControlAclEntry {
                scope: "access".into(),
                tag: "user".into(),
                id: Some(1234),
                perm: "---".into(),
            },
            crate::control::protocol::ControlAclEntry {
                scope: "access".into(),
                tag: "group_obj".into(),
                id: None,
                perm: "---".into(),
            },
            crate::control::protocol::ControlAclEntry {
                scope: "access".into(),
                tag: "mask".into(),
                id: None,
                perm: "rwx".into(),
            },
            crate::control::protocol::ControlAclEntry {
                scope: "access".into(),
                tag: "other".into(),
                id: None,
                perm: "---".into(),
            },
        ])
        .unwrap();
        meta_a
            .set_attr(
                child,
                &SetAttrRequest {
                    mode: Some(0o666),
                    ..Default::default()
                },
                SetAttrFlags::empty(),
            )
            .await
            .unwrap();
        meta_a
            .set_xattr(child, "system.brewfs.acl", &denied_control, 0)
            .await
            .unwrap();
        let before_denied = meta_b.inode_permissions(child).await.unwrap().unwrap();
        let actor = crate::meta::layer::NamespaceActor {
            uid: 1234,
            gid: 3000,
            groups: vec![2000, 3000],
        };
        let error = crate::meta::layer::scope_namespace_actor(
            Some(actor.clone()),
            meta_b.open(
                child,
                crate::meta::store::OpenFlags::WRONLY | crate::meta::store::OpenFlags::TRUNC,
            ),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, crate::meta::store::MetaError::Io(e) if e.raw_os_error() == Some(libc::EACCES))
        );
        let after_denied = meta_a.inode_permissions(child).await.unwrap().unwrap();
        assert_eq!(
            (after_denied.attr.size, after_denied.attr.ctime),
            (before_denied.attr.size, before_denied.attr.ctime)
        );
        meta_a
            .remove_xattr(child, "system.brewfs.acl")
            .await
            .unwrap();
        // A caller may belong to both the owning group and a named group.
        // Their permissions are alternatives: read from one and write from
        // the other must not combine into an O_RDWR grant.  This contract is
        // exercised through the generic KV store so the same CAS policy is
        // covered by Redis and TiKV when their ignored integration fixture is
        // enabled.
        let split_groups = serde_json::to_vec(&vec![
            crate::control::protocol::ControlAclEntry {
                scope: "access".into(),
                tag: "user_obj".into(),
                id: None,
                perm: "rwx".into(),
            },
            crate::control::protocol::ControlAclEntry {
                scope: "access".into(),
                tag: "group_obj".into(),
                id: None,
                perm: "r--".into(),
            },
            crate::control::protocol::ControlAclEntry {
                scope: "access".into(),
                tag: "group".into(),
                id: Some(3000),
                perm: "-w-".into(),
            },
            crate::control::protocol::ControlAclEntry {
                scope: "access".into(),
                tag: "mask".into(),
                id: None,
                perm: "rwx".into(),
            },
            crate::control::protocol::ControlAclEntry {
                scope: "access".into(),
                tag: "other".into(),
                id: None,
                perm: "---".into(),
            },
        ])
        .unwrap();
        meta_a
            .set_xattr(child, "system.brewfs.acl", &split_groups, 0)
            .await
            .unwrap();
        let split_error = crate::meta::layer::scope_namespace_actor(
            Some(actor.clone()),
            meta_b.open(child, crate::meta::store::OpenFlags::RDWR),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&split_error, crate::meta::store::MetaError::Io(e) if e.raw_os_error() == Some(libc::EACCES)),
            "group ACL entries must not be OR-ed across groups: {split_error:?}"
        );
        meta_a
            .remove_xattr(child, "system.brewfs.acl")
            .await
            .unwrap();
        meta_b
            .set_attr_as(
                child,
                &SetAttrRequest::default(),
                SetAttrFlags::SET_ATIME_NOW | SetAttrFlags::SET_MTIME_NOW,
                1234,
                &[3000],
            )
            .await
            .unwrap();
        let error = meta_b
            .set_attr_as(
                child,
                &SetAttrRequest {
                    atime: Some(42),
                    mtime: Some(42),
                    ..Default::default()
                },
                SetAttrFlags::empty(),
                1234,
                &[3000],
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, crate::meta::store::MetaError::Io(e) if e.raw_os_error() == Some(libc::EPERM))
        );
        let base = a
            .load_layer(guard.expected_head_layer_id)
            .await
            .unwrap()
            .parent_layer_id
            .unwrap();
        let query = PermissionSnapshotQuery {
            layer_ids: [guard.expected_head_layer_id, base],
            inodes: vec![child],
            dentry: None,
        };
        let before = b.read_permission_snapshot(query.clone()).await.unwrap();
        a.release_lease(ReleaseLease {
            lease_id: guard.lease_id,
            holder_generation: guard.holder_generation,
        })
        .await
        .unwrap();
        let error = meta_b
            .set_xattr(child, "system.posix_acl_access", &access_acl(0o640), 0)
            .await
            .unwrap_err();
        assert!(
            matches!(error, crate::meta::store::MetaError::Io(error) if error.raw_os_error() == Some(libc::ESTALE))
        );
        assert_eq!(b.read_permission_snapshot(query).await.unwrap(), before);
    }

    #[tokio::test]
    async fn independent_instances_observe_atomic_permissions_and_reject_stale_policy() {
        let (store, workspace, _lease, guard) = initialized().await;
        let peer = KvWorkspaceStore::from_arc(store.backend.clone());
        permission_backend_contract(Arc::new(store), Arc::new(peer), workspace, guard).await;
    }

    #[tokio::test]
    async fn permission_precommit_failure_leaves_layer_inode_and_acl_unchanged() {
        let (store, workspace, _lease, guard) = initialized().await;
        let base = store
            .load_layer(workspace.head_layer_id)
            .await
            .unwrap()
            .parent_layer_id
            .unwrap();
        let query = permission_query(&workspace, base);
        let before = store.read_permission_snapshot(query.clone()).await.unwrap();
        store.backend.fail_cas_once.store(true, Ordering::SeqCst);
        assert!(
            store
                .apply_permission_mutation(permission_request(&before, &guard, 0o640))
                .await
                .is_err()
        );
        assert_eq!(
            store.read_permission_snapshot(query.clone()).await.unwrap(),
            before
        );
        store
            .apply_permission_mutation(permission_request(&before, &guard, 0o640))
            .await
            .unwrap();
        let after = store.read_permission_snapshot(query).await.unwrap();
        assert_eq!(
            after.layers[0].next_sequence,
            before.layers[0].next_sequence + 2
        );
    }

    #[tokio::test]
    async fn update_control_retries_transient_census_busy_before_mutation() {
        let (store, workspace, _lease, _guard) = initialized().await;
        let generation_key = LAYER_INVENTORY_GENERATION_KEY.to_vec();
        let current = store
            .backend
            .get(&generation_key)
            .await
            .unwrap()
            .as_deref()
            .map(decode::<u64>)
            .transpose()
            .unwrap()
            .unwrap();
        // The first census authentication observes a concurrent inventory
        // generation change and returns Busy before the operation runs.
        // Generic control updates must rebuild the census and retry; this
        // mutation is deliberately outside the operation's write set.
        store.backend.mutate_on_cas.lock().await.push(KvWrite::Put {
            key: generation_key,
            value: encode(&(current + 1)).unwrap(),
        });
        let workspace_id = workspace.workspace_id;
        store
            .update_control(|state, _writes| {
                state
                    .workspaces
                    .get_mut(&workspace_id)
                    .ok_or(WorkspaceError::WorkspaceNotFound(workspace_id))?
                    .updated_at_ns += 1;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(
            store
                .load_workspace(workspace_id)
                .await
                .unwrap()
                .updated_at_ns,
            workspace.updated_at_ns + 1
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
            Arc::new(budgeted_kv_store(backend_a)),
            Arc::new(budgeted_kv_store(backend_b)),
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
            Arc::new(budgeted_kv_store(backend_a)),
            Arc::new(budgeted_kv_store(backend_b)),
        )
        .await;
    }

    mod packed_unstarted_recovery {
        include!("packed_unstarted_recovery_tests.rs");
    }

    mod g13_gc_candidates {
        include!("g13_kv_gc_tests.rs");
    }

    mod g13_real_backend_candidates {
        include!("g13_real_backend_tests.rs");
    }

    mod g10_control_open_growth_candidates {
        include!("g10_control_open_growth_tests.rs");
    }

    #[tokio::test]
    async fn native_reverse_orphan_deleting_birth_remains_collectible() {
        let (store, workspace, _lease, _guard) = initialized().await;
        let orphan = LayerId::new();
        store
            .record_orphan_slice(crate::workspace_overlay::catalog::RecordOrphanSlice {
                orphan_layer_id: orphan,
                slice_id: 712,
                slice_end: 8192,
            })
            .await
            .unwrap();
        let row = store.load_layer(orphan).await.unwrap();
        assert_eq!(row.state, LayerState::Deleting);
        assert_eq!(row.owned_bytes, 8192);
        assert!(
            store
                .backend
                .get(&native_reverse::state_key(orphan))
                .await
                .unwrap()
                .is_some()
        );
        let budget = store.packed_reader_pin_budget.get().unwrap().clone();
        let base = store
            .load_layer_chain(workspace.head_layer_id)
            .await
            .unwrap()[1]
            .clone();
        assert!(matches!(
            store
                .get_native_reverse_authority(&[row, base], budget)
                .await,
            Err(WorkspaceError::Busy)
        ));
        store
            .finalize_layer_metadata_deletion(vec![orphan])
            .await
            .unwrap();
        assert_eq!(
            store
                .backend
                .get(&native_reverse::state_key(orphan))
                .await
                .unwrap(),
            None
        );
        assert!(
            store
                .backend
                .scan_prefix(&extent_layer_prefix(orphan))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn native_reverse_namespace_duplicates_index_only_final_inode_and_whiteout() {
        let (store, workspace, _lease, guard) = initialized().await;
        let layer = workspace.head_layer_id;
        store
            .apply_namespace_mutation(NamespaceMutation {
                guard: guard.clone(),
                dentries: vec![
                    DentryDelta::put(layer, 1, b"same-\xff".to_vec(), 2, 0, 0),
                    DentryDelta::put(layer, 1, b"same-\xff".to_vec(), 3, 0, 0),
                ],
                inodes: vec![],
            })
            .await
            .unwrap();
        let budget = store.packed_reader_pin_budget.get().unwrap().clone();
        let layers = store.load_layer_chain(layer).await.unwrap();
        let proof = store
            .get_native_reverse_authority(&layers, budget.clone())
            .await
            .unwrap();
        assert!(
            store
                .get_native_reverse_dentry_page(&proof, layer, 2, None, budget.clone())
                .await
                .unwrap()
                .rows
                .is_empty()
        );
        let page = store
            .get_native_reverse_dentry_page(&proof, layer, 3, None, budget.clone())
            .await
            .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].sequence, 2);
        store
            .confirm_native_reverse_authority(&proof, budget.clone())
            .await
            .unwrap();
        drop(page);
        drop(proof);
        store
            .apply_namespace_mutation(NamespaceMutation {
                guard,
                dentries: vec![DentryDelta::whiteout(layer, 1, b"same-\xff".to_vec(), 0)],
                inodes: vec![],
            })
            .await
            .unwrap();
        let layers = store.load_layer_chain(layer).await.unwrap();
        let proof = store
            .get_native_reverse_authority(&layers, budget.clone())
            .await
            .unwrap();
        assert!(
            store
                .get_native_reverse_dentry_page(&proof, layer, 3, None, budget.clone())
                .await
                .unwrap()
                .rows
                .is_empty()
        );
        store
            .confirm_native_reverse_authority(&proof, budget)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn native_reverse_actual_gc_removes_all_builds_and_same_bytes_rebirth_gets_new_authority()
    {
        let (store, workspace, _lease, _guard) = initialized().await;
        let orphan = LayerId::new();
        let mut record = store
            .load_layer_chain(workspace.head_layer_id)
            .await
            .unwrap()[1]
            .clone();
        record.layer_id = orphan;
        record.next_sequence = 2;
        let original = record.clone();
        let row = DentryDelta::put(orphan, 1, b"orphan-\xff".to_vec(), 123, 0, 1);
        store
            .update_control_with_packed_roots(false, |state, _, writes| {
                state.layers.insert(orphan, record.clone());
                writes.push(put(dentry_key(&row), &row)?);
                Ok(())
            })
            .await
            .unwrap();
        let budget = store.packed_reader_pin_budget.get().unwrap().clone();
        for _ in 0..2 {
            store
                .start_native_reverse_index(orphan, budget.clone())
                .await
                .unwrap();
            assert!(
                !store
                    .advance_native_reverse_index(orphan, budget.clone())
                    .await
                    .unwrap()
            );
            assert!(
                store
                    .advance_native_reverse_index(orphan, budget.clone())
                    .await
                    .unwrap()
            );
        }
        let base = store
            .load_layer_chain(workspace.head_layer_id)
            .await
            .unwrap()[1]
            .clone();
        let proof = store
            .get_native_reverse_authority(&[original.clone(), base], budget.clone())
            .await
            .unwrap();
        assert_eq!(
            store
                .backend
                .scan_prefix(&native_reverse::layer_prefix(orphan))
                .await
                .unwrap()
                .len(),
            3
        );
        store
            .update_control_with_packed_roots(false, |state, _, _| {
                state.layers.get_mut(&orphan).unwrap().state = LayerState::Deleting;
                Ok(())
            })
            .await
            .unwrap();
        store
            .finalize_layer_metadata_deletion(vec![orphan])
            .await
            .unwrap();
        assert_eq!(
            store
                .backend
                .get(&native_reverse::state_key(orphan))
                .await
                .unwrap(),
            None
        );
        assert!(
            store
                .backend
                .scan_prefix(&native_reverse::layer_prefix(orphan))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .backend
                .scan_prefix(&dentry_layer_prefix(orphan))
                .await
                .unwrap()
                .is_empty()
        );
        // Exact old layer bytes may recur, but state/build and inventory cannot.
        store
            .update_control_with_packed_roots(false, |state, _, _| {
                state.layers.insert(orphan, original.clone());
                Ok(())
            })
            .await
            .unwrap();
        assert!(matches!(
            store.confirm_native_reverse_authority(&proof, budget).await,
            Err(WorkspaceError::Busy)
        ));
        assert_eq!(store.load_layer(orphan).await.unwrap(), original);
    }
    #[tokio::test]
    async fn entity_catalog_uses_header_and_one_active_lease_authority() {
        let (store, workspace, lease, _) = initialized().await;
        let header = store.backend.get(CONTROL_KEY).await.unwrap().unwrap();
        assert!(header.starts_with(CONTROL_MAGIC));
        assert!(decode_control(&header).unwrap().header.is_some());
        assert_eq!(workspace.active_lease, Some(lease.lease_id));
        assert_eq!(
            store
                .load_workspace(workspace.workspace_id)
                .await
                .unwrap()
                .active_lease,
            Some(lease.lease_id)
        );
        assert!(
            store
                .backend
                .records
                .lock()
                .await
                .keys()
                .all(|key| !key.starts_with(b"hot/"))
        );
        store
            .release_lease(ReleaseLease {
                lease_id: lease.lease_id,
                holder_generation: lease.holder_generation,
            })
            .await
            .unwrap();
        assert_eq!(
            store
                .load_workspace(workspace.workspace_id)
                .await
                .unwrap()
                .active_lease,
            None
        );
        assert_eq!(
            store.backend.get(CONTROL_KEY).await.unwrap().unwrap(),
            header
        );
    }

    #[tokio::test]
    async fn migration_marker_fences_entity_and_hot_native_packets_without_writes() {
        let (store, workspace, _, guard) = initialized().await;
        store
            .backend
            .records
            .lock()
            .await
            .insert(CONTROL_KEY.to_vec(), b"BWSMG002incomplete".to_vec());
        let before = store.backend.records.lock().await.clone();
        let mut txn = store.topology_txn();
        assert!(txn.read_workspace(workspace.workspace_id).await.is_err());
        assert!(store.allocate_id("inode").await.is_err());
        assert!(
            store
                .hot_mutation(&guard, |layer, _| {
                    allocate_layer_sequences(layer, 1)?;
                    Ok(())
                })
                .await
                .is_err()
        );
        assert_eq!(*store.backend.records.lock().await, before);
    }

    #[tokio::test]
    async fn scoped_new_lease_cannot_replace_another_workspaces_route() {
        let (store, workspace, lease, _) = initialized().await;
        let foreign = WorkspaceId::new();
        let scope = TopologyScope {
            leases: vec![(foreign, lease.lease_id)],
            ..TopologyScope::default()
        };
        let before = store.backend.records.lock().await.clone();
        assert!(matches!(
            store
                .read_topology_scope(&scope, topology_point_limits(32))
                .await,
            Err(WorkspaceError::Fenced)
        ));
        assert_eq!(*store.backend.records.lock().await, before);
        assert_eq!(
            store
                .backend
                .get(&lease_index_key(lease.lease_id))
                .await
                .unwrap(),
            Some(encode(&workspace.workspace_id).unwrap())
        );
    }

    #[tokio::test]
    async fn entity_projection_rejects_duplicate_global_ids_across_workspaces() {
        let (store, _, lease, _) = initialized().await;
        let mut foreign = lease.clone();
        foreign.workspace_id = WorkspaceId::new();
        let checks = vec![
            KvCheck {
                key: CONTROL_KEY.to_vec(),
                expected: store.backend.get(CONTROL_KEY).await.unwrap(),
            },
            KvCheck {
                key: hot_lease_key(lease.workspace_id, lease.lease_id),
                expected: Some(encode(&lease).unwrap()),
            },
            KvCheck {
                key: hot_lease_key(foreign.workspace_id, foreign.lease_id),
                expected: Some(encode(&foreign).unwrap()),
            },
        ];
        assert!(matches!(
            topology_state_from_checks(&checks),
            Err(WorkspaceError::Fenced)
        ));
    }

    #[tokio::test]
    async fn ordinary_native_sequence_write_keeps_catalog_header_and_root_epoch() {
        let (store, _, _, guard) = initialized().await;
        let header = store.backend.get(CONTROL_KEY).await.unwrap();
        let epoch = store.backend.get(TOPOLOGY_GENERATION_KEY).await.unwrap();
        let sequence = store
            .load_layer(guard.expected_head_layer_id)
            .await
            .unwrap()
            .next_sequence;
        store
            .hot_mutation(&guard, |layer, _| {
                allocate_layer_sequences(layer, 1)?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(
            store
                .load_layer(guard.expected_head_layer_id)
                .await
                .unwrap()
                .next_sequence,
            sequence + 1
        );
        assert_eq!(store.backend.get(CONTROL_KEY).await.unwrap(), header);
        assert_eq!(
            store.backend.get(TOPOLOGY_GENERATION_KEY).await.unwrap(),
            epoch
        );
    }

    include!("kv_store/extent_compaction_tests.rs");
    include!("kv_store/pr141_entity_cas_tests.rs");

    include!("kv_store/topology_write_order_tests.rs");
    include!("kv_store/public_sidecar_migration_tests.rs");
    include!("kv_store/packed_permission_migration_tests.rs");
    include!("kv_store/native_reverse_migration_tests.rs");
}
