//! SQLite implementation of the workspace catalog.

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::sync::atomic::{AtomicU8, Ordering};

use async_trait::async_trait;
use sea_orm::sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow,
};
use sea_orm::sqlx::{Row, Sqlite, SqlitePool, Transaction};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::workspace_overlay::catalog::{
    AbortSeal, AclMutation, AclQuery, AcquireLease, AdvanceSeal, AppendDataExtent, BeginSeal,
    CompactionResult, CreateSnapshot, CreateVolumeRoot, CreateWorkspace, DataMutation,
    DataMutationResult, DeleteLayerMetadata, DentryQuery, ExtentQuery, FastForwardCommit,
    GcSnapshot, HeadGuard, InodeMutation, InodeQuery, InstallCompaction, MarkDeleting,
    MutationResult, NamespaceMutation, RecordOrphanSlice, ReleaseLease, RenewLease, SliceReference,
    WorkspaceStore, WorkspaceStoreCapabilities, XattrMutation, XattrQuery,
};
use crate::workspace_overlay::digest::{CanonicalLayerDelta, delta_digest, root_hash};
use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::ids::{JournalId, LayerId, SnapshotId, WorkspaceId};
use crate::workspace_overlay::model::{
    AclDelta, BaseRevision, CommitResult, DataExtentDelta, DentryDelta, DentryOp, ExtentKind,
    InodeDelta, InodeState, LayerRecord, LayerState, LeaseState, SealJournal, SealPhase,
    SealResult, SnapshotLease, SnapshotRecord, ValueOp, VolumeHeader, WORKSPACE_SCHEMA_VERSION,
    WorkspaceRecord, WorkspaceState, XattrDelta,
};
use crate::workspace_overlay::resolver::validate_layer_chain;

const VOLUME_FORMAT: &str = "workspace-v1";

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS ws_v1_volume_header (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    volume_format TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    volume_id BLOB NOT NULL,
    created_at_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ws_v1_workspaces (
    workspace_id BLOB PRIMARY KEY,
    head_layer_id BLOB NOT NULL,
    head_epoch INTEGER NOT NULL,
    fork_base_layer_id BLOB,
    fork_base_version INTEGER,
    fork_base_root_hash BLOB,
    owner_id TEXT,
    state INTEGER NOT NULL,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ws_v1_layers (
    layer_id BLOB PRIMARY KEY,
    parent_layer_id BLOB,
    state INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    sealed_version INTEGER,
    delta_digest BLOB,
    root_hash BLOB,
    depth INTEGER NOT NULL,
    owner_workspace_id BLOB,
    next_sequence INTEGER NOT NULL,
    owned_slice_count INTEGER NOT NULL DEFAULT 0,
    owned_bytes INTEGER NOT NULL DEFAULT 0,
    created_at_ns INTEGER NOT NULL,
    sealed_at_ns INTEGER
);
CREATE INDEX IF NOT EXISTS ws_v1_layers_parent_idx ON ws_v1_layers(parent_layer_id);
CREATE TABLE IF NOT EXISTS ws_v1_dentry_delta (
    layer_id BLOB NOT NULL,
    parent_ino INTEGER NOT NULL,
    name BLOB NOT NULL,
    op INTEGER NOT NULL,
    ino INTEGER,
    entry_type INTEGER,
    sequence INTEGER NOT NULL,
    PRIMARY KEY(layer_id, parent_ino, name),
    CHECK((op = 0 AND ino IS NOT NULL AND entry_type IS NOT NULL) OR
          (op = 1 AND ino IS NULL AND entry_type IS NULL))
);
CREATE INDEX IF NOT EXISTS ws_v1_dentry_parent_idx
    ON ws_v1_dentry_delta(layer_id, parent_ino);
CREATE TABLE IF NOT EXISTS ws_v1_inode_delta (
    layer_id BLOB NOT NULL,
    ino INTEGER NOT NULL,
    state INTEGER NOT NULL,
    kind INTEGER NOT NULL,
    size INTEGER NOT NULL,
    mode INTEGER NOT NULL,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    rdev INTEGER NOT NULL,
    nlink INTEGER NOT NULL,
    atime_ns INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    ctime_ns INTEGER NOT NULL,
    symlink_target BLOB,
    parent_hint INTEGER,
    data_version INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    PRIMARY KEY(layer_id, ino)
);
CREATE TABLE IF NOT EXISTS ws_v1_xattr_delta (
    layer_id BLOB NOT NULL,
    ino INTEGER NOT NULL,
    name BLOB NOT NULL,
    op INTEGER NOT NULL,
    value BLOB,
    sequence INTEGER NOT NULL,
    PRIMARY KEY(layer_id, ino, name),
    CHECK((op = 0 AND value IS NOT NULL) OR (op = 1 AND value IS NULL))
);
CREATE TABLE IF NOT EXISTS ws_v1_acl_delta (
    layer_id BLOB NOT NULL,
    ino INTEGER NOT NULL,
    acl_type INTEGER NOT NULL,
    acl_id INTEGER NOT NULL,
    op INTEGER NOT NULL,
    value BLOB,
    sequence INTEGER NOT NULL,
    PRIMARY KEY(layer_id, ino, acl_type, acl_id),
    CHECK((op = 0 AND value IS NOT NULL) OR (op = 1 AND value IS NULL))
);
CREATE TABLE IF NOT EXISTS ws_v1_data_extent_delta (
    layer_id BLOB NOT NULL,
    ino INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    logical_offset INTEGER NOT NULL,
    length INTEGER NOT NULL CHECK(length > 0),
    kind INTEGER NOT NULL,
    slice_id INTEGER,
    slice_offset INTEGER,
    sequence INTEGER NOT NULL,
    PRIMARY KEY(layer_id, ino, chunk_index, sequence),
    CHECK((kind = 0 AND slice_id IS NOT NULL AND slice_offset IS NOT NULL) OR
          (kind = 1 AND slice_id IS NULL AND slice_offset IS NULL))
);
CREATE INDEX IF NOT EXISTS ws_v1_extent_lookup_idx
    ON ws_v1_data_extent_delta(layer_id, ino, chunk_index, sequence);
CREATE TABLE IF NOT EXISTS ws_v1_snapshot_leases (
    lease_id BLOB PRIMARY KEY,
    workspace_id BLOB NOT NULL,
    base_layer_id BLOB NOT NULL,
    base_version INTEGER NOT NULL,
    base_root_hash BLOB NOT NULL,
    holder_generation INTEGER NOT NULL,
    writable INTEGER NOT NULL,
    state INTEGER NOT NULL,
    expires_at_ns INTEGER NOT NULL,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ws_v1_one_writer_idx
    ON ws_v1_snapshot_leases(workspace_id) WHERE writable = 1 AND state = 0;
CREATE TABLE IF NOT EXISTS ws_v1_snapshots (
    snapshot_id BLOB PRIMARY KEY,
    name TEXT,
    layer_id BLOB NOT NULL,
    sealed_version INTEGER NOT NULL,
    root_hash BLOB NOT NULL,
    owner_id TEXT,
    created_at_ns INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ws_v1_snapshot_name_idx
    ON ws_v1_snapshots(name) WHERE name IS NOT NULL;
CREATE TABLE IF NOT EXISTS ws_v1_seal_journal (
    journal_id BLOB PRIMARY KEY,
    workspace_id BLOB NOT NULL,
    old_head_layer_id BLOB NOT NULL,
    expected_head_epoch INTEGER NOT NULL,
    phase INTEGER NOT NULL,
    pending_bytes INTEGER NOT NULL,
    delta_digest BLOB,
    root_hash BLOB,
    new_head_layer_id BLOB,
    last_error TEXT,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ws_v1_allocators (
    name TEXT PRIMARY KEY,
    next_value INTEGER NOT NULL
);
"#;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum StoreFailpoint {
    Disabled = 0,
    BeforeCommit = 1,
}

pub struct SqliteWorkspaceStore {
    pool: SqlitePool,
    write_gate: Arc<Mutex<()>>,
    #[cfg(test)]
    failpoint: AtomicU8,
}

impl SqliteWorkspaceStore {
    pub async fn connect(url: &str) -> Result<Self, WorkspaceError> {
        let is_memory = url.contains("::memory:");
        let mut options = SqliteConnectOptions::from_str(url)
            .map_err(backend)?
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(30));
        if !is_memory {
            options = options.journal_mode(SqliteJournalMode::Wal);
        }
        let max_connections = if is_memory { 1 } else { 8 };
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(30))
            .connect_with(options)
            .await
            .map_err(backend)?;
        Ok(Self {
            pool,
            write_gate: Arc::new(Mutex::new(())),
            #[cfg(test)]
            failpoint: AtomicU8::new(StoreFailpoint::Disabled as u8),
        })
    }

    /// Start a write transaction while holding SQLite's reserved write lock.
    ///
    /// A deferred transaction can successfully read and then fail immediately
    /// when it upgrades to a writer if another BrewFS process has modified the
    /// shared catalog. `BEGIN IMMEDIATE` acquires that lock up front, so the
    /// configured busy timeout can serialize writers across mount processes.
    async fn begin_write(&self) -> Result<Transaction<'static, Sqlite>, WorkspaceError> {
        self.pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(backend)
    }

    pub async fn schema_table_names(&self) -> Result<Vec<String>, WorkspaceError> {
        let rows = sea_orm::sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'ws_v1_%' ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.into_iter()
            .map(|row| row.try_get("name").map_err(backend))
            .collect()
    }

    #[cfg(test)]
    pub fn set_failpoint(&self, failpoint: StoreFailpoint) {
        self.failpoint.store(failpoint as u8, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub async fn journal_mode(&self) -> Result<String, WorkspaceError> {
        sea_orm::sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&self.pool)
            .await
            .map_err(backend)
    }

    async fn checked_guard(
        tx: &mut Transaction<'_, Sqlite>,
        guard: &HeadGuard,
    ) -> Result<(), WorkspaceError> {
        let now = now_ns()?;
        let matched = sea_orm::sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM ws_v1_workspaces w
             JOIN ws_v1_layers l ON l.layer_id = w.head_layer_id
             JOIN ws_v1_snapshot_leases s ON s.workspace_id = w.workspace_id
             WHERE w.workspace_id = ? AND w.state = ?
               AND w.head_layer_id = ? AND w.head_epoch = ?
               AND l.state = ? AND l.owner_workspace_id = w.workspace_id
               AND s.lease_id = ? AND s.state = ? AND s.writable = 1
               AND s.holder_generation = ? AND s.expires_at_ns > ?",
        )
        .bind(guard.workspace_id.as_bytes().as_slice())
        .bind(WorkspaceState::Active.discriminant() as i64)
        .bind(guard.expected_head_layer_id.as_bytes().as_slice())
        .bind(to_i64(guard.expected_head_epoch, "head epoch")?)
        .bind(LayerState::Writable.discriminant() as i64)
        .bind(guard.lease_id.as_bytes().as_slice())
        .bind(LeaseState::Active.discriminant() as i64)
        .bind(to_i64(guard.holder_generation, "holder generation")?)
        .bind(now)
        .fetch_one(&mut **tx)
        .await
        .map_err(backend)?;
        if matched != 1 {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    async fn allocate_sequences(
        tx: &mut Transaction<'_, Sqlite>,
        layer_id: LayerId,
        count: usize,
    ) -> Result<Option<(u64, u64)>, WorkspaceError> {
        if count == 0 {
            return Ok(None);
        }
        let first = sea_orm::sqlx::query_scalar::<_, i64>(
            "SELECT next_sequence FROM ws_v1_layers WHERE layer_id = ? AND state = ?",
        )
        .bind(layer_id.as_bytes().as_slice())
        .bind(LayerState::Writable.discriminant() as i64)
        .fetch_optional(&mut **tx)
        .await
        .map_err(backend)?
        .ok_or(WorkspaceError::Fenced)?;
        let first = to_u64(first, "next sequence")?;
        let count = u64::try_from(count)
            .map_err(|_| WorkspaceError::CorruptMetadata("mutation is too large".into()))?;
        let next = first.checked_add(count).ok_or_else(|| {
            WorkspaceError::CorruptMetadata("layer sequence allocator overflow".into())
        })?;
        let updated = sea_orm::sqlx::query(
            "UPDATE ws_v1_layers SET next_sequence = ? WHERE layer_id = ? AND next_sequence = ?",
        )
        .bind(to_i64(next, "next sequence")?)
        .bind(layer_id.as_bytes().as_slice())
        .bind(to_i64(first, "first sequence")?)
        .execute(&mut **tx)
        .await
        .map_err(backend)?;
        if updated.rows_affected() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        Ok(Some((first, next - 1)))
    }
}

#[async_trait]
impl WorkspaceStore for SqliteWorkspaceStore {
    fn name(&self) -> &'static str {
        "workspace-sqlite"
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
        let _guard = self.write_gate.lock().await;
        sea_orm::sqlx::raw_sql(SCHEMA)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn load_volume_header(&self) -> Result<Option<VolumeHeader>, WorkspaceError> {
        let row = sea_orm::sqlx::query(
            "SELECT volume_format, schema_version, volume_id, created_at_ns
             FROM ws_v1_volume_header WHERE singleton_id = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        row.map(decode_volume_header).transpose()
    }

    async fn load_workspace(&self, id: WorkspaceId) -> Result<WorkspaceRecord, WorkspaceError> {
        let row = sea_orm::sqlx::query(
            "SELECT workspace_id, head_layer_id, head_epoch,
                    fork_base_layer_id, fork_base_version, fork_base_root_hash,
                    owner_id, state, created_at_ns, updated_at_ns
             FROM ws_v1_workspaces WHERE workspace_id = ?",
        )
        .bind(id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or(WorkspaceError::WorkspaceNotFound(id))?;
        decode_workspace(&row)
    }

    async fn load_layer(&self, id: LayerId) -> Result<LayerRecord, WorkspaceError> {
        let row = sea_orm::sqlx::query(
            "SELECT layer_id, parent_layer_id, state, schema_version, sealed_version,
                    delta_digest, root_hash, depth, owner_workspace_id, next_sequence,
                    owned_slice_count, owned_bytes, created_at_ns, sealed_at_ns
             FROM ws_v1_layers WHERE layer_id = ?",
        )
        .bind(id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or(WorkspaceError::LayerNotFound(id))?;
        decode_layer(&row)
    }

    async fn load_layer_chain(&self, head: LayerId) -> Result<Vec<LayerRecord>, WorkspaceError> {
        let mut chain = Vec::new();
        let mut current = Some(head);
        while let Some(layer_id) = current {
            if chain.len() > crate::workspace_overlay::model::LAYER_CHAIN_HARD_LIMIT as usize {
                return Err(WorkspaceError::LayerDepthLimit {
                    depth: chain.len() as u32,
                    hard_limit: crate::workspace_overlay::model::LAYER_CHAIN_HARD_LIMIT,
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
        let _guard = self.write_gate.lock().await;
        sea_orm::sqlx::query_scalar::<_, i64>(
            "UPDATE ws_v1_allocators SET next_value = next_value + 1
             WHERE name = ? AND next_value < ? RETURNING next_value - 1",
        )
        .bind(name)
        .bind(i64::MAX)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| {
            WorkspaceError::CorruptMetadata(format!(
                "workspace allocator {name} is missing or exhausted"
            ))
        })
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
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let marker_exists = sea_orm::sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM ws_v1_volume_header WHERE singleton_id = 1",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;
        if marker_exists != 0 {
            return Err(WorkspaceError::InvalidStateTransition {
                from: "initialized".into(),
                to: "create-volume-root".into(),
            });
        }

        let now = now_ns()?;
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
        let root_delta = CanonicalLayerDelta {
            inodes: vec![root_inode.clone()],
            ..CanonicalLayerDelta::default()
        };
        let root_delta_digest = delta_digest(&root_delta)?;
        let root_hash = root_hash([0; 32], root_delta_digest);

        sea_orm::sqlx::query(
            "INSERT INTO ws_v1_layers
             (layer_id, parent_layer_id, state, schema_version, sealed_version,
              delta_digest, root_hash, depth, owner_workspace_id, next_sequence,
              owned_slice_count, owned_bytes, created_at_ns, sealed_at_ns)
             VALUES (?, NULL, ?, ?, 1, ?, ?, 1, NULL, 2, 0, 0, ?, ?)",
        )
        .bind(request.root_layer_id.as_bytes().as_slice())
        .bind(LayerState::Sealed.discriminant() as i64)
        .bind(WORKSPACE_SCHEMA_VERSION as i64)
        .bind(root_delta_digest.as_slice())
        .bind(root_hash.as_slice())
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        insert_inode(&mut tx, &root_inode).await?;

        sea_orm::sqlx::query(
            "INSERT INTO ws_v1_layers
             (layer_id, parent_layer_id, state, schema_version, sealed_version,
              delta_digest, root_hash, depth, owner_workspace_id, next_sequence,
              owned_slice_count, owned_bytes, created_at_ns, sealed_at_ns)
             VALUES (?, ?, ?, ?, NULL, NULL, NULL, 2, ?, 1, 0, 0, ?, NULL)",
        )
        .bind(request.writable_layer_id.as_bytes().as_slice())
        .bind(request.root_layer_id.as_bytes().as_slice())
        .bind(LayerState::Writable.discriminant() as i64)
        .bind(WORKSPACE_SCHEMA_VERSION as i64)
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;

        sea_orm::sqlx::query(
            "INSERT INTO ws_v1_workspaces
             (workspace_id, head_layer_id, head_epoch,
              fork_base_layer_id, fork_base_version, fork_base_root_hash,
              owner_id, state, created_at_ns, updated_at_ns)
             VALUES (?, ?, 0, ?, 1, ?, ?, ?, ?, ?)",
        )
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(request.writable_layer_id.as_bytes().as_slice())
        .bind(request.root_layer_id.as_bytes().as_slice())
        .bind(root_hash.as_slice())
        .bind(request.owner_id.as_deref())
        .bind(WorkspaceState::Active.discriminant() as i64)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        for (name, next_value) in [
            ("inode", 2_i64),
            ("slice", 1_i64),
            ("sealed_version", 2_i64),
        ] {
            sea_orm::sqlx::query("INSERT INTO ws_v1_allocators(name, next_value) VALUES (?, ?)")
                .bind(name)
                .bind(next_value)
                .execute(&mut *tx)
                .await
                .map_err(backend)?;
        }

        // The immutable marker is deliberately the last row written. A crash
        // before this point leaves a retryable, unmarked workspace schema.
        sea_orm::sqlx::query(
            "INSERT INTO ws_v1_volume_header
             (singleton_id, volume_format, schema_version, volume_id, created_at_ns)
             VALUES (1, ?, ?, ?, ?)",
        )
        .bind(VOLUME_FORMAT)
        .bind(WORKSPACE_SCHEMA_VERSION as i64)
        .bind(request.volume_id.as_bytes().as_slice())
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;

        Ok(WorkspaceRecord {
            workspace_id: request.workspace_id,
            head_layer_id: request.writable_layer_id,
            head_epoch: 0,
            fork_base: Some(BaseRevision {
                layer_id: request.root_layer_id,
                sealed_version: 1,
                root_hash,
            }),
            owner_id: request.owner_id,
            state: WorkspaceState::Active,
            created_at_ns: now,
            updated_at_ns: now,
        })
    }

    async fn create_workspace(
        &self,
        request: CreateWorkspace,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let base = load_revision_tx(&mut tx, request.base_revision.layer_id).await?;
        if base != request.base_revision {
            return Err(WorkspaceError::Conflict(
                crate::workspace_overlay::error::ConflictDetail {
                    path: Vec::new(),
                    reason: "fork base revision changed".into(),
                },
            ));
        }
        let parent = sea_orm::sqlx::query(
            "SELECT parent_layer_id, depth FROM ws_v1_layers WHERE layer_id = ? AND state = ?",
        )
        .bind(request.base_revision.layer_id.as_bytes().as_slice())
        .bind(LayerState::Sealed.discriminant() as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?
        .ok_or(WorkspaceError::LayerNotFound(
            request.base_revision.layer_id,
        ))?;
        let parent_layer_id = parent
            .try_get::<Option<Vec<u8>>, _>("parent_layer_id")
            .map_err(backend)?;
        let parent_depth = to_u32(
            parent.try_get("depth").map_err(backend)?,
            "parent layer depth",
        )?;
        if parent_layer_id.is_some() || parent_depth != 1 {
            return Err(WorkspaceError::CorruptMetadata(
                "workspace base revision must be a flat sealed layer".into(),
            ));
        }
        let now = now_ns()?;
        insert_writable_layer(
            &mut tx,
            request.head_layer_id,
            request.base_revision.layer_id,
            2,
            request.workspace_id,
            now,
        )
        .await?;
        sea_orm::sqlx::query(
            "INSERT INTO ws_v1_workspaces
             (workspace_id, head_layer_id, head_epoch, fork_base_layer_id,
              fork_base_version, fork_base_root_hash, owner_id, state,
              created_at_ns, updated_at_ns)
             VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(request.head_layer_id.as_bytes().as_slice())
        .bind(request.base_revision.layer_id.as_bytes().as_slice())
        .bind(to_i64(
            request.base_revision.sealed_version,
            "sealed version",
        )?)
        .bind(request.base_revision.root_hash.as_slice())
        .bind(request.owner_id.as_deref())
        .bind(WorkspaceState::Active.discriminant() as i64)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            if is_unique_violation(&error) {
                WorkspaceError::Conflict(crate::workspace_overlay::error::ConflictDetail {
                    path: Vec::new(),
                    reason: "workspace already exists".into(),
                })
            } else {
                backend(error)
            }
        })?;
        tx.commit().await.map_err(backend)?;
        Ok(WorkspaceRecord {
            workspace_id: request.workspace_id,
            head_layer_id: request.head_layer_id,
            head_epoch: 0,
            fork_base: Some(request.base_revision),
            owner_id: request.owner_id,
            state: WorkspaceState::Active,
            created_at_ns: now,
            updated_at_ns: now,
        })
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceError> {
        let rows = sea_orm::sqlx::query(
            "SELECT workspace_id, head_layer_id, head_epoch,
                    fork_base_layer_id, fork_base_version, fork_base_root_hash,
                    owner_id, state, created_at_ns, updated_at_ns
             FROM ws_v1_workspaces ORDER BY created_at_ns, workspace_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.iter().map(decode_workspace).collect()
    }

    async fn create_snapshot(
        &self,
        request: CreateSnapshot,
    ) -> Result<SnapshotRecord, WorkspaceError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        if load_revision_tx(&mut tx, request.revision.layer_id).await? != request.revision {
            return Err(WorkspaceError::Conflict(
                crate::workspace_overlay::error::ConflictDetail {
                    path: Vec::new(),
                    reason: "snapshot revision changed".into(),
                },
            ));
        }
        let now = now_ns()?;
        let result = sea_orm::sqlx::query(
            "INSERT INTO ws_v1_snapshots
             (snapshot_id, name, layer_id, sealed_version, root_hash, owner_id, created_at_ns)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(request.snapshot_id.as_bytes().as_slice())
        .bind(request.name.as_deref())
        .bind(request.revision.layer_id.as_bytes().as_slice())
        .bind(to_i64(request.revision.sealed_version, "sealed version")?)
        .bind(request.revision.root_hash.as_slice())
        .bind(request.owner_id.as_deref())
        .bind(now)
        .execute(&mut *tx)
        .await;
        if let Err(error) = result {
            if is_unique_violation(&error) {
                return Err(WorkspaceError::Conflict(
                    crate::workspace_overlay::error::ConflictDetail {
                        path: Vec::new(),
                        reason: "snapshot ID or name already exists".into(),
                    },
                ));
            }
            return Err(backend(error));
        }
        tx.commit().await.map_err(backend)?;
        Ok(SnapshotRecord {
            snapshot_id: request.snapshot_id,
            name: request.name,
            revision: request.revision,
            owner_id: request.owner_id,
            created_at_ns: now,
        })
    }

    async fn load_snapshot(&self, id: SnapshotId) -> Result<SnapshotRecord, WorkspaceError> {
        let row = sea_orm::sqlx::query(
            "SELECT snapshot_id, name, layer_id, sealed_version, root_hash,
                    owner_id, created_at_ns
             FROM ws_v1_snapshots WHERE snapshot_id = ?",
        )
        .bind(id.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| WorkspaceError::Backend(format!("snapshot not found: {id}")))?;
        decode_snapshot(&row)
    }

    async fn list_snapshots(&self) -> Result<Vec<SnapshotRecord>, WorkspaceError> {
        let rows = sea_orm::sqlx::query(
            "SELECT snapshot_id, name, layer_id, sealed_version, root_hash,
                    owner_id, created_at_ns
             FROM ws_v1_snapshots ORDER BY created_at_ns, snapshot_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.iter().map(decode_snapshot).collect()
    }

    async fn delete_snapshot(&self, id: SnapshotId) -> Result<(), WorkspaceError> {
        let _guard = self.write_gate.lock().await;
        let result = sea_orm::sqlx::query("DELETE FROM ws_v1_snapshots WHERE snapshot_id = ?")
            .bind(id.as_bytes().as_slice())
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        if result.rows_affected() != 1 {
            return Err(WorkspaceError::Backend(format!("snapshot not found: {id}")));
        }
        Ok(())
    }

    async fn acquire_lease(&self, request: AcquireLease) -> Result<SnapshotLease, WorkspaceError> {
        if request.ttl_ns == 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "lease TTL must be positive".into(),
            ));
        }
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let row = sea_orm::sqlx::query(
            "SELECT w.state AS workspace_state, l.parent_layer_id,
                    p.sealed_version, p.root_hash
             FROM ws_v1_workspaces w
             JOIN ws_v1_layers l ON l.layer_id = w.head_layer_id
             JOIN ws_v1_layers p ON p.layer_id = l.parent_layer_id
             WHERE w.workspace_id = ? AND l.state = ? AND p.state = ?",
        )
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(LayerState::Writable.discriminant() as i64)
        .bind(LayerState::Sealed.discriminant() as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?
        .ok_or(WorkspaceError::WorkspaceNotFound(request.workspace_id))?;
        let workspace_state = WorkspaceState::try_from(to_u8(
            row.try_get::<i64, _>("workspace_state").map_err(backend)?,
            "workspace state",
        )?)?;
        if workspace_state != WorkspaceState::Active {
            return Err(WorkspaceError::Busy);
        }
        let base_revision = BaseRevision {
            layer_id: LayerId::from_uuid(uuid_from_blob(
                row.try_get("parent_layer_id").map_err(backend)?,
                "lease base layer",
            )?),
            sealed_version: to_u64(
                row.try_get("sealed_version").map_err(backend)?,
                "sealed version",
            )?,
            root_hash: hash_from_blob(
                row.try_get("root_hash").map_err(backend)?,
                "lease base root hash",
            )?,
        };
        let now = now_ns()?;
        sea_orm::sqlx::query(
            "UPDATE ws_v1_snapshot_leases SET state = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND state = ? AND expires_at_ns <= ?",
        )
        .bind(LeaseState::Expired.discriminant() as i64)
        .bind(now)
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(LeaseState::Active.discriminant() as i64)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        let expires = now
            .checked_add(to_i64(request.ttl_ns, "lease TTL")?)
            .ok_or_else(|| WorkspaceError::CorruptMetadata("lease expiry overflows".into()))?;
        let insert = sea_orm::sqlx::query(
            "INSERT INTO ws_v1_snapshot_leases
             (lease_id, workspace_id, base_layer_id, base_version, base_root_hash,
              holder_generation, writable, state, expires_at_ns, created_at_ns, updated_at_ns)
             VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)",
        )
        .bind(request.lease_id.as_bytes().as_slice())
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(base_revision.layer_id.as_bytes().as_slice())
        .bind(to_i64(base_revision.sealed_version, "sealed version")?)
        .bind(base_revision.root_hash.as_slice())
        .bind(to_i64(request.holder_generation, "holder generation")?)
        .bind(LeaseState::Active.discriminant() as i64)
        .bind(expires)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await;
        if let Err(error) = insert {
            if is_unique_violation(&error) {
                return Err(WorkspaceError::Busy);
            }
            return Err(backend(error));
        }
        tx.commit().await.map_err(backend)?;
        Ok(SnapshotLease {
            lease_id: request.lease_id,
            workspace_id: request.workspace_id,
            base_revision,
            holder_generation: request.holder_generation,
            writable: true,
            state: LeaseState::Active,
            expires_at_ns: expires,
            created_at_ns: now,
            updated_at_ns: now,
        })
    }

    async fn renew_lease(&self, request: RenewLease) -> Result<SnapshotLease, WorkspaceError> {
        if request.ttl_ns == 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "lease TTL must be positive".into(),
            ));
        }
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let now = now_ns()?;
        let expires = now
            .checked_add(to_i64(request.ttl_ns, "lease TTL")?)
            .ok_or_else(|| WorkspaceError::CorruptMetadata("lease expiry overflows".into()))?;
        let row = sea_orm::sqlx::query(
            "UPDATE ws_v1_snapshot_leases
             SET expires_at_ns = ?, updated_at_ns = ?
             WHERE lease_id = ? AND holder_generation = ? AND state = ?
               AND expires_at_ns > ?
             RETURNING lease_id, workspace_id, base_layer_id, base_version,
                       base_root_hash, holder_generation, writable, state,
                       expires_at_ns, created_at_ns, updated_at_ns",
        )
        .bind(expires)
        .bind(now)
        .bind(request.lease_id.as_bytes().as_slice())
        .bind(to_i64(request.holder_generation, "holder generation")?)
        .bind(LeaseState::Active.discriminant() as i64)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?
        .ok_or(WorkspaceError::Fenced)?;
        let lease = decode_lease(&row)?;
        tx.commit().await.map_err(backend)?;
        Ok(lease)
    }

    async fn release_lease(&self, request: ReleaseLease) -> Result<(), WorkspaceError> {
        let _guard = self.write_gate.lock().await;
        let now = now_ns()?;
        let updated = sea_orm::sqlx::query(
            "UPDATE ws_v1_snapshot_leases SET state = ?, updated_at_ns = ?
             WHERE lease_id = ? AND holder_generation = ? AND state = ?",
        )
        .bind(LeaseState::Released.discriminant() as i64)
        .bind(now)
        .bind(request.lease_id.as_bytes().as_slice())
        .bind(to_i64(request.holder_generation, "holder generation")?)
        .bind(LeaseState::Active.discriminant() as i64)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        if updated.rows_affected() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    async fn reap_expired_leases(&self) -> Result<u64, WorkspaceError> {
        let _guard = self.write_gate.lock().await;
        let now = now_ns()?;
        let updated = sea_orm::sqlx::query(
            "UPDATE ws_v1_snapshot_leases SET state = ?, updated_at_ns = ?
             WHERE state = ? AND expires_at_ns <= ?",
        )
        .bind(LeaseState::Expired.discriminant() as i64)
        .bind(now)
        .bind(LeaseState::Active.discriminant() as i64)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(updated.rows_affected())
    }

    async fn list_leases(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<SnapshotLease>, WorkspaceError> {
        let rows = sea_orm::sqlx::query(
            "SELECT lease_id, workspace_id, base_layer_id, base_version,
                    base_root_hash, holder_generation, writable, state,
                    expires_at_ns, created_at_ns, updated_at_ns
             FROM ws_v1_snapshot_leases WHERE workspace_id = ?
             ORDER BY created_at_ns, lease_id",
        )
        .bind(workspace_id.as_bytes().as_slice())
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.iter().map(decode_lease).collect()
    }

    async fn get_dentry_deltas(
        &self,
        request: DentryQuery,
    ) -> Result<Vec<DentryDelta>, WorkspaceError> {
        let mut rows = Vec::new();
        for layer_id in request.layer_ids {
            let found = if let Some(name) = request.name.as_deref() {
                sea_orm::sqlx::query(
                    "SELECT layer_id, parent_ino, name, op, ino, entry_type, sequence
                     FROM ws_v1_dentry_delta
                     WHERE layer_id = ? AND parent_ino = ? AND name = ?",
                )
                .bind(layer_id.as_bytes().as_slice())
                .bind(request.parent_ino)
                .bind(name)
                .fetch_all(&self.pool)
                .await
                .map_err(backend)?
            } else {
                sea_orm::sqlx::query(
                    "SELECT layer_id, parent_ino, name, op, ino, entry_type, sequence
                     FROM ws_v1_dentry_delta
                     WHERE layer_id = ? AND parent_ino = ? ORDER BY name",
                )
                .bind(layer_id.as_bytes().as_slice())
                .bind(request.parent_ino)
                .fetch_all(&self.pool)
                .await
                .map_err(backend)?
            };
            rows.extend(
                found
                    .into_iter()
                    .map(decode_dentry)
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(rows)
    }

    async fn get_inode_deltas(
        &self,
        request: InodeQuery,
    ) -> Result<Vec<InodeDelta>, WorkspaceError> {
        let mut rows = Vec::new();
        for layer_id in request.layer_ids {
            if let Some(row) = sea_orm::sqlx::query(
                "SELECT layer_id, ino, state, kind, size, mode, uid, gid, rdev, nlink,
                        atime_ns, mtime_ns, ctime_ns, symlink_target, parent_hint,
                        data_version, sequence
                 FROM ws_v1_inode_delta WHERE layer_id = ? AND ino = ?",
            )
            .bind(layer_id.as_bytes().as_slice())
            .bind(request.ino)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?
            {
                rows.push(decode_inode(&row)?);
            }
        }
        Ok(rows)
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
        let chunk_index = to_i64(request.chunk_index, "chunk index")?;
        let range_start = to_i64(request.range_start, "range start")?;
        let range_end = to_i64(request.range_end, "range end")?;
        let mut rows = Vec::new();
        for layer_id in request.layer_ids {
            let found = sea_orm::sqlx::query(
                "SELECT layer_id, ino, chunk_index, logical_offset, length, kind,
                        slice_id, slice_offset, sequence
                 FROM ws_v1_data_extent_delta
                 WHERE layer_id = ? AND ino = ? AND chunk_index = ?
                   AND logical_offset < ? AND logical_offset + length > ?
                 ORDER BY sequence DESC",
            )
            .bind(layer_id.as_bytes().as_slice())
            .bind(request.ino)
            .bind(chunk_index)
            .bind(range_end)
            .bind(range_start)
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
            rows.extend(
                found
                    .iter()
                    .map(decode_extent)
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(rows)
    }

    async fn get_xattr_deltas(
        &self,
        request: XattrQuery,
    ) -> Result<Vec<XattrDelta>, WorkspaceError> {
        let mut rows = Vec::new();
        for layer_id in request.layer_ids {
            let found = if let Some(name) = request.name.as_deref() {
                sea_orm::sqlx::query(
                    "SELECT layer_id, ino, name, op, value, sequence
                     FROM ws_v1_xattr_delta WHERE layer_id = ? AND ino = ? AND name = ?",
                )
                .bind(layer_id.as_bytes().as_slice())
                .bind(request.ino)
                .bind(name)
                .fetch_all(&self.pool)
                .await
                .map_err(backend)?
            } else {
                sea_orm::sqlx::query(
                    "SELECT layer_id, ino, name, op, value, sequence
                     FROM ws_v1_xattr_delta WHERE layer_id = ? AND ino = ? ORDER BY name",
                )
                .bind(layer_id.as_bytes().as_slice())
                .bind(request.ino)
                .fetch_all(&self.pool)
                .await
                .map_err(backend)?
            };
            rows.extend(
                found
                    .iter()
                    .map(decode_xattr)
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(rows)
    }

    async fn get_acl_deltas(&self, request: AclQuery) -> Result<Vec<AclDelta>, WorkspaceError> {
        if request.acl_type.is_some() != request.acl_id.is_some() {
            return Err(WorkspaceError::CorruptMetadata(
                "ACL type and ID filters must be provided together".into(),
            ));
        }
        let mut rows = Vec::new();
        for layer_id in request.layer_ids {
            let found = if let (Some(acl_type), Some(acl_id)) = (request.acl_type, request.acl_id) {
                sea_orm::sqlx::query(
                    "SELECT layer_id, ino, acl_type, acl_id, op, value, sequence
                     FROM ws_v1_acl_delta
                     WHERE layer_id = ? AND ino = ? AND acl_type = ? AND acl_id = ?",
                )
                .bind(layer_id.as_bytes().as_slice())
                .bind(request.ino)
                .bind(i64::from(acl_type))
                .bind(acl_id)
                .fetch_all(&self.pool)
                .await
                .map_err(backend)?
            } else {
                sea_orm::sqlx::query(
                    "SELECT layer_id, ino, acl_type, acl_id, op, value, sequence
                     FROM ws_v1_acl_delta WHERE layer_id = ? AND ino = ?
                     ORDER BY acl_type, acl_id",
                )
                .bind(layer_id.as_bytes().as_slice())
                .bind(request.ino)
                .fetch_all(&self.pool)
                .await
                .map_err(backend)?
            };
            rows.extend(
                found
                    .iter()
                    .map(decode_acl)
                    .collect::<Result<Vec<_>, _>>()?,
            );
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

        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        Self::checked_guard(&mut tx, &request.guard).await?;
        let count = request
            .dentries
            .len()
            .checked_add(request.inodes.len())
            .ok_or_else(|| WorkspaceError::CorruptMetadata("mutation is too large".into()))?;
        let range = Self::allocate_sequences(&mut tx, request.guard.expected_head_layer_id, count)
            .await?
            .expect("non-empty mutation has a sequence range");
        let mut sequence = range.0;
        for mut dentry in request.dentries {
            dentry.sequence = sequence;
            upsert_dentry(&mut tx, &dentry).await?;
            sequence += 1;
        }
        for mut inode in request.inodes {
            inode.sequence = sequence;
            upsert_inode(&mut tx, &inode).await?;
            sequence += 1;
        }

        #[cfg(test)]
        if self.failpoint.load(Ordering::SeqCst) == StoreFailpoint::BeforeCommit as u8 {
            return Err(WorkspaceError::Backend(
                "injected failure before transaction commit".into(),
            ));
        }

        tx.commit().await.map_err(backend)?;
        Ok(MutationResult {
            first_sequence: Some(range.0),
            last_sequence: Some(range.1),
        })
    }

    async fn apply_inode_mutation(
        &self,
        request: InodeMutation,
    ) -> Result<InodeDelta, WorkspaceError> {
        let mut inode = request.inode;
        if inode.layer_id != request.guard.expected_head_layer_id {
            return Err(WorkspaceError::Fenced);
        }
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        Self::checked_guard(&mut tx, &request.guard).await?;
        let (sequence, _) = Self::allocate_sequences(&mut tx, inode.layer_id, 1)
            .await?
            .expect("single mutation has a sequence");
        inode.sequence = sequence;
        upsert_inode(&mut tx, &inode).await?;
        tx.commit().await.map_err(backend)?;
        Ok(inode)
    }

    async fn append_data_extent(
        &self,
        request: AppendDataExtent,
    ) -> Result<DataExtentDelta, WorkspaceError> {
        let mut extent = request.extent;
        if extent.layer_id != request.guard.expected_head_layer_id {
            return Err(WorkspaceError::Fenced);
        }
        extent.validate()?;
        let end = extent
            .logical_offset
            .checked_add(extent.length)
            .ok_or_else(|| WorkspaceError::CorruptMetadata("extent range overflows".into()))?;
        if end > request.chunk_size {
            return Err(WorkspaceError::CorruptMetadata(format!(
                "extent end {end} exceeds chunk size {}",
                request.chunk_size
            )));
        }
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        Self::checked_guard(&mut tx, &request.guard).await?;
        let (sequence, _) = Self::allocate_sequences(&mut tx, extent.layer_id, 1)
            .await?
            .expect("single mutation has a sequence");
        extent.sequence = sequence;
        insert_extent(&mut tx, &extent).await?;
        if matches!(extent.kind, ExtentKind::Data { .. }) {
            sea_orm::sqlx::query(
                "UPDATE ws_v1_layers
                 SET owned_slice_count = owned_slice_count + 1,
                     owned_bytes = owned_bytes + ?
                 WHERE layer_id = ?",
            )
            .bind(to_i64(extent.length, "extent length")?)
            .bind(extent.layer_id.as_bytes().as_slice())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        tx.commit().await.map_err(backend)?;
        Ok(extent)
    }

    async fn apply_data_mutation(
        &self,
        request: DataMutation,
    ) -> Result<DataMutationResult, WorkspaceError> {
        let DataMutation {
            guard,
            mut inode,
            mut extents,
            chunk_size,
        } = request;
        let head = guard.expected_head_layer_id;
        if inode.layer_id != head
            || extents
                .iter()
                .any(|extent| extent.layer_id != head || extent.ino != inode.ino)
        {
            return Err(WorkspaceError::Fenced);
        }
        for extent in &extents {
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
        }

        let sequence_count = extents
            .len()
            .checked_add(1)
            .ok_or_else(|| WorkspaceError::Backend("too many data mutations".into()))?;
        let _write_guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        Self::checked_guard(&mut tx, &guard).await?;
        let (first, last) = Self::allocate_sequences(&mut tx, head, sequence_count)
            .await?
            .expect("data mutation always allocates an inode sequence");
        inode.sequence = first;
        upsert_inode(&mut tx, &inode).await?;

        let mut owned_slice_count = 0u64;
        let mut owned_bytes = 0u64;
        for (index, extent) in extents.iter_mut().enumerate() {
            extent.sequence = first
                .checked_add(
                    u64::try_from(index)
                        .map_err(|_| WorkspaceError::Backend("extent index exceeds u64".into()))?,
                )
                .and_then(|sequence| sequence.checked_add(1))
                .ok_or_else(|| WorkspaceError::Backend("extent sequence overflows".into()))?;
            insert_extent(&mut tx, extent).await?;
            if matches!(extent.kind, ExtentKind::Data { .. }) {
                owned_slice_count = owned_slice_count
                    .checked_add(1)
                    .ok_or_else(|| WorkspaceError::Backend("owned slice count overflows".into()))?;
                owned_bytes = owned_bytes
                    .checked_add(extent.length)
                    .ok_or_else(|| WorkspaceError::Backend("owned byte count overflows".into()))?;
            }
        }
        debug_assert_eq!(
            extents.last().map(|extent| extent.sequence),
            (!extents.is_empty()).then_some(last)
        );
        if owned_slice_count != 0 {
            sea_orm::sqlx::query(
                "UPDATE ws_v1_layers
                 SET owned_slice_count = owned_slice_count + ?,
                     owned_bytes = owned_bytes + ?
                 WHERE layer_id = ?",
            )
            .bind(to_i64(owned_slice_count, "owned slice count")?)
            .bind(to_i64(owned_bytes, "owned bytes")?)
            .bind(head.as_bytes().as_slice())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        tx.commit().await.map_err(backend)?;
        Ok(DataMutationResult { inode, extents })
    }

    async fn apply_xattr_mutation(&self, request: XattrMutation) -> Result<(), WorkspaceError> {
        let mut xattr = request.xattr;
        let mut inode = request.inode;
        validate_value(xattr.op, xattr.value.as_deref(), "xattr")?;
        if xattr.layer_id != request.guard.expected_head_layer_id
            || inode.layer_id != request.guard.expected_head_layer_id
            || inode.ino != xattr.ino
        {
            return Err(WorkspaceError::Fenced);
        }
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        Self::checked_guard(&mut tx, &request.guard).await?;
        let (first_sequence, last_sequence) = Self::allocate_sequences(&mut tx, xattr.layer_id, 2)
            .await?
            .expect("xattr mutation has a sequence range");
        xattr.sequence = first_sequence;
        inode.sequence = last_sequence;
        upsert_xattr(&mut tx, &xattr).await?;
        upsert_inode(&mut tx, &inode).await?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn apply_acl_mutation(&self, request: AclMutation) -> Result<(), WorkspaceError> {
        let mut acl = request.acl;
        validate_value(acl.op, acl.value.as_deref(), "ACL")?;
        if acl.layer_id != request.guard.expected_head_layer_id {
            return Err(WorkspaceError::Fenced);
        }
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        Self::checked_guard(&mut tx, &request.guard).await?;
        let (sequence, _) = Self::allocate_sequences(&mut tx, acl.layer_id, 1)
            .await?
            .expect("single mutation has a sequence");
        acl.sequence = sequence;
        upsert_acl(&mut tx, &acl).await?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn load_layer_delta(
        &self,
        layer_id: LayerId,
    ) -> Result<CanonicalLayerDelta, WorkspaceError> {
        self.load_layer(layer_id).await?;
        load_layer_delta_pool(&self.pool, layer_id).await
    }

    async fn begin_seal(&self, request: BeginSeal) -> Result<SealJournal, WorkspaceError> {
        if request.new_head_layer_id == request.guard.expected_head_layer_id {
            return Err(WorkspaceError::CorruptMetadata(
                "seal new head must differ from old head".into(),
            ));
        }
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        Self::checked_guard(&mut tx, &request.guard).await?;
        let now = now_ns()?;
        let workspace = sea_orm::sqlx::query(
            "UPDATE ws_v1_workspaces SET state = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND state = ? AND head_layer_id = ? AND head_epoch = ?
             RETURNING workspace_id",
        )
        .bind(WorkspaceState::Sealing.discriminant() as i64)
        .bind(now)
        .bind(request.guard.workspace_id.as_bytes().as_slice())
        .bind(WorkspaceState::Active.discriminant() as i64)
        .bind(request.guard.expected_head_layer_id.as_bytes().as_slice())
        .bind(to_i64(request.guard.expected_head_epoch, "head epoch")?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?;
        if workspace.is_none() {
            return Err(WorkspaceError::Fenced);
        }
        let changed = sea_orm::sqlx::query(
            "UPDATE ws_v1_layers SET state = ?
             WHERE layer_id = ? AND state = ? AND owner_workspace_id = ?",
        )
        .bind(LayerState::Sealing.discriminant() as i64)
        .bind(request.guard.expected_head_layer_id.as_bytes().as_slice())
        .bind(LayerState::Writable.discriminant() as i64)
        .bind(request.guard.workspace_id.as_bytes().as_slice())
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        if changed.rows_affected() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        sea_orm::sqlx::query(
            "INSERT INTO ws_v1_seal_journal
             (journal_id, workspace_id, old_head_layer_id, expected_head_epoch,
              phase, pending_bytes, delta_digest, root_hash, new_head_layer_id,
              last_error, created_at_ns, updated_at_ns)
             VALUES (?, ?, ?, ?, ?, 0, NULL, NULL, ?, NULL, ?, ?)",
        )
        .bind(request.journal_id.as_bytes().as_slice())
        .bind(request.guard.workspace_id.as_bytes().as_slice())
        .bind(request.guard.expected_head_layer_id.as_bytes().as_slice())
        .bind(to_i64(request.guard.expected_head_epoch, "head epoch")?)
        .bind(SealPhase::Prepare.discriminant() as i64)
        .bind(request.new_head_layer_id.as_bytes().as_slice())
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(SealJournal {
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
        })
    }

    async fn advance_seal(&self, request: AdvanceSeal) -> Result<SealJournal, WorkspaceError> {
        let allowed = matches!(
            (request.expected_phase, request.next_phase),
            (SealPhase::Prepare, SealPhase::Quiesced)
                | (SealPhase::Quiesced, SealPhase::DataDrained)
                | (SealPhase::HeadSwitched, SealPhase::Completed)
        );
        if !allowed {
            return Err(WorkspaceError::InvalidStateTransition {
                from: format!("{:?}", request.expected_phase),
                to: format!("{:?}", request.next_phase),
            });
        }
        let _guard = self.write_gate.lock().await;
        let now = now_ns()?;
        let pending = request
            .pending_bytes
            .map(|value| to_i64(value, "pending bytes"));
        let pending = pending.transpose()?;
        let row = sea_orm::sqlx::query(
            "UPDATE ws_v1_seal_journal
             SET phase = ?, pending_bytes = COALESCE(?, pending_bytes),
                 last_error = ?, updated_at_ns = ?
             WHERE journal_id = ? AND phase = ?
             RETURNING journal_id, workspace_id, old_head_layer_id,
                       expected_head_epoch, phase, pending_bytes, delta_digest,
                       root_hash, new_head_layer_id, last_error, created_at_ns, updated_at_ns",
        )
        .bind(request.next_phase.discriminant() as i64)
        .bind(pending)
        .bind(request.last_error.as_deref())
        .bind(now)
        .bind(request.journal_id.as_bytes().as_slice())
        .bind(request.expected_phase.discriminant() as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| WorkspaceError::InvalidStateTransition {
            from: format!("not {:?}", request.expected_phase),
            to: format!("{:?}", request.next_phase),
        })?;
        decode_seal_journal(&row)
    }

    async fn hash_seal(&self, journal_id: JournalId) -> Result<SealJournal, WorkspaceError> {
        let _guard = self.write_gate.lock().await;
        let current = load_seal_journal_pool(&self.pool, journal_id).await?;
        if current.phase != SealPhase::DataDrained {
            return Err(WorkspaceError::InvalidStateTransition {
                from: format!("{:?}", current.phase),
                to: "Hashed".into(),
            });
        }
        let delta = load_layer_delta_pool(&self.pool, current.old_head_layer_id).await?;
        let digest = delta_digest(&delta)?;
        let layer = self.load_layer(current.old_head_layer_id).await?;
        let parent_hash = match layer.parent_layer_id {
            Some(parent) => self.load_layer(parent).await?.root_hash.ok_or_else(|| {
                WorkspaceError::CorruptMetadata("sealed parent has no root hash".into())
            })?,
            None => [0; 32],
        };
        let root = root_hash(parent_hash, digest);
        let now = now_ns()?;
        let row = sea_orm::sqlx::query(
            "UPDATE ws_v1_seal_journal
             SET phase = ?, delta_digest = ?, root_hash = ?, updated_at_ns = ?
             WHERE journal_id = ? AND phase = ?
             RETURNING journal_id, workspace_id, old_head_layer_id,
                       expected_head_epoch, phase, pending_bytes, delta_digest,
                       root_hash, new_head_layer_id, last_error, created_at_ns, updated_at_ns",
        )
        .bind(SealPhase::Hashed.discriminant() as i64)
        .bind(digest.as_slice())
        .bind(root.as_slice())
        .bind(now)
        .bind(journal_id.as_bytes().as_slice())
        .bind(SealPhase::DataDrained.discriminant() as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or(WorkspaceError::Fenced)?;
        decode_seal_journal(&row)
    }

    async fn commit_seal(&self, journal_id: JournalId) -> Result<SealResult, WorkspaceError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let row = load_seal_journal_tx(&mut tx, journal_id).await?;
        if row.phase == SealPhase::Completed {
            let old = load_revision_tx(&mut tx, row.old_head_layer_id).await?;
            let workspace = load_workspace_tx(&mut tx, row.workspace_id).await?;
            return Ok(SealResult {
                revision: old,
                new_head_layer_id: workspace.head_layer_id,
                head_epoch: workspace.head_epoch,
            });
        }
        if row.phase == SealPhase::HeadSwitched {
            let old = load_revision_tx(&mut tx, row.old_head_layer_id).await?;
            let workspace = load_workspace_tx(&mut tx, row.workspace_id).await?;
            let now = now_ns()?;
            sea_orm::sqlx::query(
                "UPDATE ws_v1_seal_journal SET phase = ?, updated_at_ns = ?
                 WHERE journal_id = ? AND phase = ?",
            )
            .bind(SealPhase::Completed.discriminant() as i64)
            .bind(now)
            .bind(journal_id.as_bytes().as_slice())
            .bind(SealPhase::HeadSwitched.discriminant() as i64)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
            tx.commit().await.map_err(backend)?;
            return Ok(SealResult {
                revision: old,
                new_head_layer_id: workspace.head_layer_id,
                head_epoch: workspace.head_epoch,
            });
        }
        if row.phase != SealPhase::Hashed {
            return Err(WorkspaceError::InvalidStateTransition {
                from: format!("{:?}", row.phase),
                to: "HeadSwitched".into(),
            });
        }
        let digest = row
            .delta_digest
            .ok_or_else(|| WorkspaceError::CorruptMetadata("hashed journal lacks digest".into()))?;
        let root = row.root_hash.ok_or_else(|| {
            WorkspaceError::CorruptMetadata("hashed journal lacks root hash".into())
        })?;
        let new_head = row
            .new_head_layer_id
            .ok_or_else(|| WorkspaceError::CorruptMetadata("seal journal lacks new head".into()))?;
        let old_layer_row =
            sea_orm::sqlx::query("SELECT depth FROM ws_v1_layers WHERE layer_id = ? AND state = ?")
                .bind(row.old_head_layer_id.as_bytes().as_slice())
                .bind(LayerState::Sealing.discriminant() as i64)
                .fetch_optional(&mut *tx)
                .await
                .map_err(backend)?
                .ok_or(WorkspaceError::Fenced)?;
        let new_depth = to_u32(
            old_layer_row.try_get("depth").map_err(backend)?,
            "layer depth",
        )?
        .checked_add(1)
        .ok_or_else(|| WorkspaceError::CorruptMetadata("layer depth overflows".into()))?;
        if new_depth > crate::workspace_overlay::model::LAYER_CHAIN_HARD_LIMIT {
            return Err(WorkspaceError::LayerDepthLimit {
                depth: new_depth,
                hard_limit: crate::workspace_overlay::model::LAYER_CHAIN_HARD_LIMIT,
            });
        }
        let sealed_version = allocate_id_tx(&mut tx, "sealed_version").await?;
        let sealed_version = to_u64(sealed_version, "sealed version")?;
        let now = now_ns()?;
        let updated = sea_orm::sqlx::query(
            "UPDATE ws_v1_layers
             SET state = ?, sealed_version = ?, delta_digest = ?, root_hash = ?,
                 owner_workspace_id = NULL, sealed_at_ns = ?
             WHERE layer_id = ? AND state = ?",
        )
        .bind(LayerState::Sealed.discriminant() as i64)
        .bind(to_i64(sealed_version, "sealed version")?)
        .bind(digest.as_slice())
        .bind(root.as_slice())
        .bind(now)
        .bind(row.old_head_layer_id.as_bytes().as_slice())
        .bind(LayerState::Sealing.discriminant() as i64)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        if updated.rows_affected() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        insert_writable_layer(
            &mut tx,
            new_head,
            row.old_head_layer_id,
            new_depth,
            row.workspace_id,
            now,
        )
        .await?;
        let new_epoch = row
            .expected_head_epoch
            .checked_add(1)
            .ok_or_else(|| WorkspaceError::CorruptMetadata("head epoch overflows".into()))?;
        let switched = sea_orm::sqlx::query(
            "UPDATE ws_v1_workspaces
             SET head_layer_id = ?, head_epoch = ?, state = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND head_layer_id = ? AND head_epoch = ? AND state = ?",
        )
        .bind(new_head.as_bytes().as_slice())
        .bind(to_i64(new_epoch, "head epoch")?)
        .bind(WorkspaceState::Active.discriminant() as i64)
        .bind(now)
        .bind(row.workspace_id.as_bytes().as_slice())
        .bind(row.old_head_layer_id.as_bytes().as_slice())
        .bind(to_i64(row.expected_head_epoch, "head epoch")?)
        .bind(WorkspaceState::Sealing.discriminant() as i64)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        if switched.rows_affected() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        sea_orm::sqlx::query(
            "UPDATE ws_v1_snapshot_leases
             SET base_layer_id = ?, base_version = ?, base_root_hash = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND state = ?",
        )
        .bind(row.old_head_layer_id.as_bytes().as_slice())
        .bind(to_i64(sealed_version, "sealed version")?)
        .bind(root.as_slice())
        .bind(now)
        .bind(row.workspace_id.as_bytes().as_slice())
        .bind(LeaseState::Active.discriminant() as i64)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sea_orm::sqlx::query(
            "UPDATE ws_v1_seal_journal SET phase = ?, updated_at_ns = ?
             WHERE journal_id = ? AND phase = ?",
        )
        .bind(SealPhase::Completed.discriminant() as i64)
        .bind(now)
        .bind(journal_id.as_bytes().as_slice())
        .bind(SealPhase::Hashed.discriminant() as i64)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(SealResult {
            revision: BaseRevision {
                layer_id: row.old_head_layer_id,
                sealed_version,
                root_hash: root,
            },
            new_head_layer_id: new_head,
            head_epoch: new_epoch,
        })
    }

    async fn abort_recoverable_seal(&self, request: AbortSeal) -> Result<(), WorkspaceError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let journal = load_seal_journal_tx(&mut tx, request.journal_id).await?;
        if !matches!(
            journal.phase,
            SealPhase::Prepare | SealPhase::Quiesced | SealPhase::DataDrained
        ) {
            return Err(WorkspaceError::InvalidStateTransition {
                from: format!("{:?}", journal.phase),
                to: "Aborted".into(),
            });
        }
        let now = now_ns()?;
        sea_orm::sqlx::query("UPDATE ws_v1_layers SET state = ? WHERE layer_id = ? AND state = ?")
            .bind(LayerState::Writable.discriminant() as i64)
            .bind(journal.old_head_layer_id.as_bytes().as_slice())
            .bind(LayerState::Sealing.discriminant() as i64)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        sea_orm::sqlx::query(
            "UPDATE ws_v1_workspaces SET state = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND head_layer_id = ? AND head_epoch = ?",
        )
        .bind(WorkspaceState::Active.discriminant() as i64)
        .bind(now)
        .bind(journal.workspace_id.as_bytes().as_slice())
        .bind(journal.old_head_layer_id.as_bytes().as_slice())
        .bind(to_i64(journal.expected_head_epoch, "head epoch")?)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sea_orm::sqlx::query(
            "UPDATE ws_v1_seal_journal SET phase = ?, last_error = ?, updated_at_ns = ?
             WHERE journal_id = ?",
        )
        .bind(SealPhase::Aborted.discriminant() as i64)
        .bind(request.reason)
        .bind(now)
        .bind(request.journal_id.as_bytes().as_slice())
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn load_seal_journal(
        &self,
        journal_id: JournalId,
    ) -> Result<SealJournal, WorkspaceError> {
        load_seal_journal_pool(&self.pool, journal_id).await
    }

    async fn list_incomplete_seal_journals(&self) -> Result<Vec<SealJournal>, WorkspaceError> {
        let rows = sea_orm::sqlx::query(
            "SELECT journal_id, workspace_id, old_head_layer_id, expected_head_epoch,
                    phase, pending_bytes, delta_digest, root_hash, new_head_layer_id,
                    last_error, created_at_ns, updated_at_ns
             FROM ws_v1_seal_journal WHERE phase NOT IN (?, ?) ORDER BY created_at_ns",
        )
        .bind(SealPhase::Completed.discriminant() as i64)
        .bind(SealPhase::Aborted.discriminant() as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.iter().map(decode_seal_journal).collect()
    }

    async fn fast_forward_commit(
        &self,
        request: FastForwardCommit,
    ) -> Result<CommitResult, WorkspaceError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        if load_revision_tx(&mut tx, request.source_revision.layer_id).await?
            != request.source_revision
        {
            return Err(commit_conflict("source revision changed"));
        }
        let target = load_workspace_tx(&mut tx, request.target_workspace_id).await?;
        if target.head_layer_id != request.target_expected_head_layer_id
            || target.head_epoch != request.target_expected_head_epoch
            || target.state != WorkspaceState::Active
            || target.fork_base.as_ref() != Some(&request.source_fork_base)
        {
            return Err(commit_conflict("target revision changed"));
        }
        let target_head = sea_orm::sqlx::query(
            "SELECT parent_layer_id, next_sequence, depth FROM ws_v1_layers
             WHERE layer_id = ? AND state = ? AND owner_workspace_id = ?",
        )
        .bind(target.head_layer_id.as_bytes().as_slice())
        .bind(LayerState::Writable.discriminant() as i64)
        .bind(target.workspace_id.as_bytes().as_slice())
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?
        .ok_or_else(|| commit_conflict("target head is not writable"))?;
        if target_head
            .try_get::<i64, _>("next_sequence")
            .map_err(backend)?
            != 1
        {
            return Err(commit_conflict("target writable head is not empty"));
        }
        let parent = LayerId::from_uuid(uuid_from_blob(
            target_head.try_get("parent_layer_id").map_err(backend)?,
            "target parent layer",
        )?);
        if load_revision_tx(&mut tx, parent).await? != request.source_fork_base {
            return Err(commit_conflict("target base revision changed"));
        }
        let now = now_ns()?;
        let active_lease = sea_orm::sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM ws_v1_snapshot_leases
             WHERE workspace_id = ? AND writable = 1 AND state = ? AND expires_at_ns > ?",
        )
        .bind(target.workspace_id.as_bytes().as_slice())
        .bind(LeaseState::Active.discriminant() as i64)
        .bind(now)
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;
        if active_lease != 0 {
            return Err(commit_conflict("target has an active writable lease"));
        }
        let source_depth = sea_orm::sqlx::query_scalar::<_, i64>(
            "SELECT depth FROM ws_v1_layers WHERE layer_id = ?",
        )
        .bind(request.source_revision.layer_id.as_bytes().as_slice())
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;
        let depth = to_u32(source_depth, "source depth")?
            .checked_add(1)
            .ok_or_else(|| WorkspaceError::CorruptMetadata("layer depth overflows".into()))?;
        if depth > crate::workspace_overlay::model::LAYER_CHAIN_HARD_LIMIT {
            return Err(WorkspaceError::LayerDepthLimit {
                depth,
                hard_limit: crate::workspace_overlay::model::LAYER_CHAIN_HARD_LIMIT,
            });
        }
        insert_writable_layer(
            &mut tx,
            request.new_head_layer_id,
            request.source_revision.layer_id,
            depth,
            target.workspace_id,
            now,
        )
        .await?;
        let epoch = target
            .head_epoch
            .checked_add(1)
            .ok_or_else(|| WorkspaceError::CorruptMetadata("head epoch overflows".into()))?;
        sea_orm::sqlx::query(
            "UPDATE ws_v1_workspaces
             SET head_layer_id = ?, head_epoch = ?, fork_base_layer_id = ?,
                 fork_base_version = ?, fork_base_root_hash = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND head_layer_id = ? AND head_epoch = ?",
        )
        .bind(request.new_head_layer_id.as_bytes().as_slice())
        .bind(to_i64(epoch, "head epoch")?)
        .bind(request.source_revision.layer_id.as_bytes().as_slice())
        .bind(to_i64(
            request.source_revision.sealed_version,
            "sealed version",
        )?)
        .bind(request.source_revision.root_hash.as_slice())
        .bind(now)
        .bind(target.workspace_id.as_bytes().as_slice())
        .bind(target.head_layer_id.as_bytes().as_slice())
        .bind(to_i64(target.head_epoch, "head epoch")?)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sea_orm::sqlx::query(
            "UPDATE ws_v1_layers SET state = ?, owner_workspace_id = NULL
             WHERE layer_id = ? AND state = ?",
        )
        .bind(LayerState::Deleting.discriminant() as i64)
        .bind(target.head_layer_id.as_bytes().as_slice())
        .bind(LayerState::Writable.discriminant() as i64)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(CommitResult {
            revision: request.source_revision,
            target_head_layer_id: request.new_head_layer_id,
            target_head_epoch: epoch,
        })
    }

    async fn mark_workspace_deleting(&self, request: MarkDeleting) -> Result<(), WorkspaceError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let now = now_ns()?;
        let active = sea_orm::sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM ws_v1_snapshot_leases
             WHERE workspace_id = ? AND state = ? AND expires_at_ns > ?",
        )
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(LeaseState::Active.discriminant() as i64)
        .bind(now)
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;
        if active != 0 && !request.force_fence_lease {
            return Err(WorkspaceError::Busy);
        }
        if request.force_fence_lease {
            sea_orm::sqlx::query(
                "UPDATE ws_v1_snapshot_leases SET state = ?, updated_at_ns = ?
                 WHERE workspace_id = ? AND state = ?",
            )
            .bind(LeaseState::Released.discriminant() as i64)
            .bind(now)
            .bind(request.workspace_id.as_bytes().as_slice())
            .bind(LeaseState::Active.discriminant() as i64)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        let row = sea_orm::sqlx::query(
            "UPDATE ws_v1_workspaces SET state = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND state = ? RETURNING head_layer_id",
        )
        .bind(WorkspaceState::Deleting.discriminant() as i64)
        .bind(now)
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(WorkspaceState::Active.discriminant() as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?
        .ok_or(WorkspaceError::WorkspaceNotFound(request.workspace_id))?;
        let head = row
            .try_get::<Vec<u8>, _>("head_layer_id")
            .map_err(backend)?;
        sea_orm::sqlx::query(
            "UPDATE ws_v1_layers SET state = ?, owner_workspace_id = NULL
             WHERE layer_id = ? AND state = ?",
        )
        .bind(LayerState::Deleting.discriminant() as i64)
        .bind(head)
        .bind(LayerState::Writable.discriminant() as i64)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn record_orphan_slice(&self, request: RecordOrphanSlice) -> Result<(), WorkspaceError> {
        if request.slice_end == 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "orphan slice length must be non-zero".into(),
            ));
        }
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let now = now_ns()?;
        sea_orm::sqlx::query(
            "INSERT INTO ws_v1_layers
             (layer_id, parent_layer_id, state, schema_version, sealed_version,
              delta_digest, root_hash, depth, owner_workspace_id, next_sequence,
              owned_slice_count, owned_bytes, created_at_ns, sealed_at_ns)
             VALUES (?, NULL, ?, ?, NULL, NULL, NULL, 1, NULL, 2, 1, ?, ?, NULL)",
        )
        .bind(request.orphan_layer_id.as_bytes().as_slice())
        .bind(LayerState::Deleting.discriminant() as i64)
        .bind(WORKSPACE_SCHEMA_VERSION as i64)
        .bind(to_i64(request.slice_end, "orphan slice end")?)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        insert_extent(
            &mut tx,
            &DataExtentDelta::data(
                request.orphan_layer_id,
                1,
                0,
                0,
                request.slice_end,
                request.slice_id,
                0,
                1,
            ),
        )
        .await?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn gc_snapshot(
        &self,
        now_ns: i64,
        lease_grace_ns: u64,
    ) -> Result<GcSnapshot, WorkspaceError> {
        let grace = to_i64(lease_grace_ns, "lease grace")?;
        let lease_cutoff = now_ns.saturating_sub(grace);
        let mut roots = BTreeSet::new();
        let workspace_roots =
            sea_orm::sqlx::query("SELECT head_layer_id FROM ws_v1_workspaces WHERE state != ?")
                .bind(WorkspaceState::Deleting.discriminant() as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(backend)?;
        for row in workspace_roots {
            roots.insert(LayerId::from_uuid(uuid_from_blob(
                row.try_get("head_layer_id").map_err(backend)?,
                "workspace GC root",
            )?));
        }
        let lease_roots = sea_orm::sqlx::query(
            "SELECT base_layer_id FROM ws_v1_snapshot_leases
             WHERE state IN (?, ?) AND expires_at_ns > ?",
        )
        .bind(LeaseState::Active.discriminant() as i64)
        .bind(LeaseState::Releasing.discriminant() as i64)
        .bind(lease_cutoff)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        for row in lease_roots {
            roots.insert(LayerId::from_uuid(uuid_from_blob(
                row.try_get("base_layer_id").map_err(backend)?,
                "lease GC root",
            )?));
        }
        let snapshot_roots = sea_orm::sqlx::query("SELECT layer_id FROM ws_v1_snapshots")
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
        for row in snapshot_roots {
            roots.insert(LayerId::from_uuid(uuid_from_blob(
                row.try_get("layer_id").map_err(backend)?,
                "snapshot GC root",
            )?));
        }
        let journal_roots = sea_orm::sqlx::query(
            "SELECT old_head_layer_id, new_head_layer_id FROM ws_v1_seal_journal
             WHERE phase NOT IN (?, ?)",
        )
        .bind(SealPhase::Completed.discriminant() as i64)
        .bind(SealPhase::Aborted.discriminant() as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        for row in journal_roots {
            roots.insert(LayerId::from_uuid(uuid_from_blob(
                row.try_get("old_head_layer_id").map_err(backend)?,
                "journal old-head GC root",
            )?));
            if let Some(bytes) = row
                .try_get::<Option<Vec<u8>>, _>("new_head_layer_id")
                .map_err(backend)?
            {
                roots.insert(LayerId::from_uuid(uuid_from_blob(
                    bytes,
                    "journal new-head GC root",
                )?));
            }
        }
        let layer_rows = sea_orm::sqlx::query(
            "SELECT layer_id, parent_layer_id, state, schema_version, sealed_version,
                    delta_digest, root_hash, depth, owner_workspace_id, next_sequence,
                    owned_slice_count, owned_bytes, created_at_ns, sealed_at_ns
             FROM ws_v1_layers ORDER BY created_at_ns, layer_id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let layers = layer_rows
            .iter()
            .map(decode_layer)
            .collect::<Result<Vec<_>, _>>()?;
        let slice_rows = sea_orm::sqlx::query(
            "SELECT layer_id, slice_id, slice_offset, length
             FROM ws_v1_data_extent_delta WHERE kind = 0",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut slice_references = Vec::with_capacity(slice_rows.len());
        for row in slice_rows {
            let slice_offset = to_u64(
                row.try_get("slice_offset").map_err(backend)?,
                "slice offset",
            )?;
            let length = to_u64(row.try_get("length").map_err(backend)?, "extent length")?;
            slice_references.push(SliceReference {
                layer_id: LayerId::from_uuid(uuid_from_blob(
                    row.try_get("layer_id").map_err(backend)?,
                    "slice reference layer",
                )?),
                slice_id: to_u64(row.try_get("slice_id").map_err(backend)?, "slice ID")?,
                slice_end: slice_offset.checked_add(length).ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("slice reference overflows".into())
                })?,
            });
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
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let lease_cutoff = request
            .now_ns
            .saturating_sub(to_i64(request.lease_grace_ns, "lease grace")?);
        for layer_id in &request.layer_ids {
            let reachable = sea_orm::sqlx::query_scalar::<_, i64>(
                "WITH RECURSIVE roots(layer_id) AS (
                     SELECT head_layer_id FROM ws_v1_workspaces WHERE state != ?
                     UNION SELECT layer_id FROM ws_v1_snapshots
                     UNION SELECT base_layer_id FROM ws_v1_snapshot_leases
                       WHERE state IN (?, ?) AND expires_at_ns > ?
                     UNION SELECT old_head_layer_id FROM ws_v1_seal_journal
                       WHERE phase NOT IN (?, ?)
                     UNION SELECT new_head_layer_id FROM ws_v1_seal_journal
                       WHERE phase NOT IN (?, ?) AND new_head_layer_id IS NOT NULL
                 ), reachable(layer_id) AS (
                     SELECT layer_id FROM roots
                     UNION
                     SELECT l.parent_layer_id FROM ws_v1_layers l
                     JOIN reachable r ON l.layer_id = r.layer_id
                     WHERE l.parent_layer_id IS NOT NULL
                 ) SELECT COUNT(*) FROM reachable WHERE layer_id = ?",
            )
            .bind(WorkspaceState::Deleting.discriminant() as i64)
            .bind(LeaseState::Active.discriminant() as i64)
            .bind(LeaseState::Releasing.discriminant() as i64)
            .bind(lease_cutoff)
            .bind(SealPhase::Completed.discriminant() as i64)
            .bind(SealPhase::Aborted.discriminant() as i64)
            .bind(SealPhase::Completed.discriminant() as i64)
            .bind(SealPhase::Aborted.discriminant() as i64)
            .bind(layer_id.as_bytes().as_slice())
            .fetch_one(&mut *tx)
            .await
            .map_err(backend)?;
            if reachable != 0 {
                return Err(WorkspaceError::Busy);
            }
        }
        for layer_id in &request.layer_ids {
            sea_orm::sqlx::query(
                "UPDATE ws_v1_layers SET state = ?, owner_workspace_id = NULL
                 WHERE layer_id = ?",
            )
            .bind(LayerState::Deleting.discriminant() as i64)
            .bind(layer_id.as_bytes().as_slice())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn finalize_layer_metadata_deletion(
        &self,
        layer_ids: Vec<LayerId>,
    ) -> Result<(), WorkspaceError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        for layer_id in &layer_ids {
            let state = sea_orm::sqlx::query_scalar::<_, i64>(
                "SELECT state FROM ws_v1_layers WHERE layer_id = ?",
            )
            .bind(layer_id.as_bytes().as_slice())
            .fetch_optional(&mut *tx)
            .await
            .map_err(backend)?;
            if state.is_some_and(|state| state != LayerState::Deleting.discriminant() as i64) {
                return Err(WorkspaceError::Busy);
            }
            for table in [
                "ws_v1_dentry_delta",
                "ws_v1_inode_delta",
                "ws_v1_xattr_delta",
                "ws_v1_acl_delta",
                "ws_v1_data_extent_delta",
            ] {
                let sql = format!("DELETE FROM {table} WHERE layer_id = ?");
                sea_orm::sqlx::query(&sql)
                    .bind(layer_id.as_bytes().as_slice())
                    .execute(&mut *tx)
                    .await
                    .map_err(backend)?;
            }
            sea_orm::sqlx::query("DELETE FROM ws_v1_layers WHERE layer_id = ? AND state = ?")
                .bind(layer_id.as_bytes().as_slice())
                .bind(LayerState::Deleting.discriminant() as i64)
                .execute(&mut *tx)
                .await
                .map_err(backend)?;
        }
        tx.commit().await.map_err(backend)?;
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
        let max_sequence = request
            .delta
            .dentries
            .iter()
            .map(|row| row.sequence)
            .chain(request.delta.inodes.iter().map(|row| row.sequence))
            .chain(request.delta.xattrs.iter().map(|row| row.sequence))
            .chain(request.delta.acls.iter().map(|row| row.sequence))
            .chain(request.delta.extents.iter().map(|row| row.sequence))
            .max()
            .unwrap_or(0);
        let next_sequence = max_sequence
            .checked_add(1)
            .ok_or_else(|| WorkspaceError::CorruptMetadata("sequence overflows".into()))?;

        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let now = now_ns()?;
        let head = sea_orm::sqlx::query(
            "SELECT parent_layer_id, next_sequence FROM ws_v1_layers
             WHERE layer_id = ? AND state = ? AND owner_workspace_id = ?",
        )
        .bind(request.expected_head_layer_id.as_bytes().as_slice())
        .bind(LayerState::Writable.discriminant() as i64)
        .bind(request.workspace_id.as_bytes().as_slice())
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?
        .ok_or(WorkspaceError::Fenced)?;
        let parent = LayerId::from_uuid(uuid_from_blob(
            head.try_get("parent_layer_id").map_err(backend)?,
            "compaction head parent",
        )?);
        if parent != request.expected_parent_layer_id
            || head.try_get::<i64, _>("next_sequence").map_err(backend)? != 1
        {
            return Err(WorkspaceError::Fenced);
        }
        let workspace = load_workspace_tx(&mut tx, request.workspace_id).await?;
        if workspace.head_layer_id != request.expected_head_layer_id
            || workspace.head_epoch != request.expected_head_epoch
            || workspace.state != WorkspaceState::Active
        {
            return Err(WorkspaceError::Fenced);
        }
        let sealed_version = to_u64(
            allocate_id_tx(&mut tx, "sealed_version").await?,
            "sealed version",
        )?;
        sea_orm::sqlx::query(
            "INSERT INTO ws_v1_layers
             (layer_id, parent_layer_id, state, schema_version, sealed_version,
              delta_digest, root_hash, depth, owner_workspace_id, next_sequence,
              owned_slice_count, owned_bytes, created_at_ns, sealed_at_ns)
             VALUES (?, NULL, ?, ?, ?, ?, ?, 1, NULL, ?, 0, 0, ?, ?)",
        )
        .bind(request.compacted_layer_id.as_bytes().as_slice())
        .bind(LayerState::Sealed.discriminant() as i64)
        .bind(WORKSPACE_SCHEMA_VERSION as i64)
        .bind(to_i64(sealed_version, "sealed version")?)
        .bind(digest.as_slice())
        .bind(root.as_slice())
        .bind(to_i64(next_sequence, "next sequence")?)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        for row in &request.delta.dentries {
            upsert_dentry(&mut tx, row).await?;
        }
        for row in &request.delta.inodes {
            upsert_inode(&mut tx, row).await?;
        }
        for row in &request.delta.xattrs {
            upsert_xattr(&mut tx, row).await?;
        }
        for row in &request.delta.acls {
            upsert_acl(&mut tx, row).await?;
        }
        for row in &request.delta.extents {
            insert_extent(&mut tx, row).await?;
        }
        insert_writable_layer(
            &mut tx,
            request.replacement_head_layer_id,
            request.compacted_layer_id,
            2,
            request.workspace_id,
            now,
        )
        .await?;
        let epoch = workspace
            .head_epoch
            .checked_add(1)
            .ok_or_else(|| WorkspaceError::CorruptMetadata("head epoch overflows".into()))?;
        let switched = sea_orm::sqlx::query(
            "UPDATE ws_v1_workspaces
             SET head_layer_id = ?, head_epoch = ?, fork_base_layer_id = ?,
                 fork_base_version = ?, fork_base_root_hash = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND head_layer_id = ? AND head_epoch = ? AND state = ?",
        )
        .bind(request.replacement_head_layer_id.as_bytes().as_slice())
        .bind(to_i64(epoch, "head epoch")?)
        .bind(request.compacted_layer_id.as_bytes().as_slice())
        .bind(to_i64(sealed_version, "sealed version")?)
        .bind(root.as_slice())
        .bind(now)
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(request.expected_head_layer_id.as_bytes().as_slice())
        .bind(to_i64(request.expected_head_epoch, "head epoch")?)
        .bind(WorkspaceState::Active.discriminant() as i64)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        if switched.rows_affected() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        sea_orm::sqlx::query(
            "UPDATE ws_v1_snapshot_leases
             SET base_layer_id = ?, base_version = ?, base_root_hash = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND state = ?",
        )
        .bind(request.compacted_layer_id.as_bytes().as_slice())
        .bind(to_i64(sealed_version, "sealed version")?)
        .bind(root.as_slice())
        .bind(now)
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(LeaseState::Active.discriminant() as i64)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sea_orm::sqlx::query(
            "UPDATE ws_v1_layers SET state = ?, owner_workspace_id = NULL
             WHERE layer_id = ? AND state = ?",
        )
        .bind(LayerState::Deleting.discriminant() as i64)
        .bind(request.expected_head_layer_id.as_bytes().as_slice())
        .bind(LayerState::Writable.discriminant() as i64)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(CompactionResult {
            revision: BaseRevision {
                layer_id: request.compacted_layer_id,
                sealed_version,
                root_hash: root,
            },
            replacement_head_layer_id: request.replacement_head_layer_id,
            head_epoch: epoch,
        })
    }
}

async fn insert_writable_layer(
    tx: &mut Transaction<'_, Sqlite>,
    layer_id: LayerId,
    parent_layer_id: LayerId,
    depth: u32,
    workspace_id: WorkspaceId,
    now: i64,
) -> Result<(), WorkspaceError> {
    sea_orm::sqlx::query(
        "INSERT INTO ws_v1_layers
         (layer_id, parent_layer_id, state, schema_version, sealed_version,
          delta_digest, root_hash, depth, owner_workspace_id, next_sequence,
          owned_slice_count, owned_bytes, created_at_ns, sealed_at_ns)
         VALUES (?, ?, ?, ?, NULL, NULL, NULL, ?, ?, 1, 0, 0, ?, NULL)",
    )
    .bind(layer_id.as_bytes().as_slice())
    .bind(parent_layer_id.as_bytes().as_slice())
    .bind(LayerState::Writable.discriminant() as i64)
    .bind(WORKSPACE_SCHEMA_VERSION as i64)
    .bind(i64::from(depth))
    .bind(workspace_id.as_bytes().as_slice())
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(backend)?;
    Ok(())
}

async fn allocate_id_tx(
    tx: &mut Transaction<'_, Sqlite>,
    name: &str,
) -> Result<i64, WorkspaceError> {
    sea_orm::sqlx::query_scalar::<_, i64>(
        "UPDATE ws_v1_allocators SET next_value = next_value + 1
         WHERE name = ? AND next_value < ? RETURNING next_value - 1",
    )
    .bind(name)
    .bind(i64::MAX)
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?
    .ok_or_else(|| {
        WorkspaceError::CorruptMetadata(format!(
            "workspace allocator {name} is missing or exhausted"
        ))
    })
}

async fn load_revision_tx(
    tx: &mut Transaction<'_, Sqlite>,
    layer_id: LayerId,
) -> Result<BaseRevision, WorkspaceError> {
    let row = sea_orm::sqlx::query(
        "SELECT state, sealed_version, root_hash FROM ws_v1_layers WHERE layer_id = ?",
    )
    .bind(layer_id.as_bytes().as_slice())
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?
    .ok_or(WorkspaceError::LayerNotFound(layer_id))?;
    let state = LayerState::try_from(to_u8(
        row.try_get::<i64, _>("state").map_err(backend)?,
        "layer state",
    )?)?;
    if state != LayerState::Sealed {
        return Err(commit_conflict("revision layer is not sealed"));
    }
    Ok(BaseRevision {
        layer_id,
        sealed_version: to_u64(
            row.try_get::<Option<i64>, _>("sealed_version")
                .map_err(backend)?
                .ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("sealed layer lacks version".into())
                })?,
            "sealed version",
        )?,
        root_hash: hash_from_blob(
            row.try_get::<Option<Vec<u8>>, _>("root_hash")
                .map_err(backend)?
                .ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("sealed layer lacks root hash".into())
                })?,
            "root hash",
        )?,
    })
}

async fn load_workspace_tx(
    tx: &mut Transaction<'_, Sqlite>,
    workspace_id: WorkspaceId,
) -> Result<WorkspaceRecord, WorkspaceError> {
    let row = sea_orm::sqlx::query(
        "SELECT workspace_id, head_layer_id, head_epoch,
                fork_base_layer_id, fork_base_version, fork_base_root_hash,
                owner_id, state, created_at_ns, updated_at_ns
         FROM ws_v1_workspaces WHERE workspace_id = ?",
    )
    .bind(workspace_id.as_bytes().as_slice())
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?
    .ok_or(WorkspaceError::WorkspaceNotFound(workspace_id))?;
    decode_workspace(&row)
}

async fn load_seal_journal_tx(
    tx: &mut Transaction<'_, Sqlite>,
    journal_id: JournalId,
) -> Result<SealJournal, WorkspaceError> {
    let row = sea_orm::sqlx::query(
        "SELECT journal_id, workspace_id, old_head_layer_id, expected_head_epoch,
                phase, pending_bytes, delta_digest, root_hash, new_head_layer_id,
                last_error, created_at_ns, updated_at_ns
         FROM ws_v1_seal_journal WHERE journal_id = ?",
    )
    .bind(journal_id.as_bytes().as_slice())
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?
    .ok_or_else(|| WorkspaceError::Backend(format!("seal journal not found: {journal_id}")))?;
    decode_seal_journal(&row)
}

async fn load_seal_journal_pool(
    pool: &SqlitePool,
    journal_id: JournalId,
) -> Result<SealJournal, WorkspaceError> {
    let row = sea_orm::sqlx::query(
        "SELECT journal_id, workspace_id, old_head_layer_id, expected_head_epoch,
                phase, pending_bytes, delta_digest, root_hash, new_head_layer_id,
                last_error, created_at_ns, updated_at_ns
         FROM ws_v1_seal_journal WHERE journal_id = ?",
    )
    .bind(journal_id.as_bytes().as_slice())
    .fetch_optional(pool)
    .await
    .map_err(backend)?
    .ok_or_else(|| WorkspaceError::Backend(format!("seal journal not found: {journal_id}")))?;
    decode_seal_journal(&row)
}

async fn load_layer_delta_pool(
    pool: &SqlitePool,
    layer_id: LayerId,
) -> Result<CanonicalLayerDelta, WorkspaceError> {
    let dentries = sea_orm::sqlx::query(
        "SELECT layer_id, parent_ino, name, op, ino, entry_type, sequence
         FROM ws_v1_dentry_delta WHERE layer_id = ?",
    )
    .bind(layer_id.as_bytes().as_slice())
    .fetch_all(pool)
    .await
    .map_err(backend)?
    .into_iter()
    .map(decode_dentry)
    .collect::<Result<Vec<_>, _>>()?;
    let inode_rows = sea_orm::sqlx::query(
        "SELECT layer_id, ino, state, kind, size, mode, uid, gid, rdev, nlink,
                atime_ns, mtime_ns, ctime_ns, symlink_target, parent_hint,
                data_version, sequence
         FROM ws_v1_inode_delta WHERE layer_id = ?",
    )
    .bind(layer_id.as_bytes().as_slice())
    .fetch_all(pool)
    .await
    .map_err(backend)?;
    let inodes = inode_rows
        .iter()
        .map(decode_inode)
        .collect::<Result<Vec<_>, _>>()?;
    let xattr_rows = sea_orm::sqlx::query(
        "SELECT layer_id, ino, name, op, value, sequence
         FROM ws_v1_xattr_delta WHERE layer_id = ?",
    )
    .bind(layer_id.as_bytes().as_slice())
    .fetch_all(pool)
    .await
    .map_err(backend)?;
    let xattrs = xattr_rows
        .iter()
        .map(decode_xattr)
        .collect::<Result<Vec<_>, _>>()?;
    let acl_rows = sea_orm::sqlx::query(
        "SELECT layer_id, ino, acl_type, acl_id, op, value, sequence
         FROM ws_v1_acl_delta WHERE layer_id = ?",
    )
    .bind(layer_id.as_bytes().as_slice())
    .fetch_all(pool)
    .await
    .map_err(backend)?;
    let acls = acl_rows
        .iter()
        .map(decode_acl)
        .collect::<Result<Vec<_>, _>>()?;
    let extent_rows = sea_orm::sqlx::query(
        "SELECT layer_id, ino, chunk_index, logical_offset, length, kind,
                slice_id, slice_offset, sequence
         FROM ws_v1_data_extent_delta WHERE layer_id = ?",
    )
    .bind(layer_id.as_bytes().as_slice())
    .fetch_all(pool)
    .await
    .map_err(backend)?;
    let extents = extent_rows
        .iter()
        .map(decode_extent)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CanonicalLayerDelta {
        dentries,
        inodes,
        xattrs,
        acls,
        extents,
    })
}

fn commit_conflict(reason: impl Into<String>) -> WorkspaceError {
    WorkspaceError::Conflict(crate::workspace_overlay::error::ConflictDetail {
        path: Vec::new(),
        reason: reason.into(),
    })
}

async fn upsert_dentry(
    tx: &mut Transaction<'_, Sqlite>,
    row: &DentryDelta,
) -> Result<(), WorkspaceError> {
    sea_orm::sqlx::query(
        "INSERT INTO ws_v1_dentry_delta
         (layer_id, parent_ino, name, op, ino, entry_type, sequence)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(layer_id, parent_ino, name) DO UPDATE SET
             op = excluded.op, ino = excluded.ino, entry_type = excluded.entry_type,
             sequence = excluded.sequence",
    )
    .bind(row.layer_id.as_bytes().as_slice())
    .bind(row.parent_ino)
    .bind(row.name.as_slice())
    .bind(row.op.discriminant() as i64)
    .bind(row.ino)
    .bind(row.entry_type.map(i64::from))
    .bind(to_i64(row.sequence, "dentry sequence")?)
    .execute(&mut **tx)
    .await
    .map_err(backend)?;
    Ok(())
}

async fn insert_inode(
    tx: &mut Transaction<'_, Sqlite>,
    row: &InodeDelta,
) -> Result<(), WorkspaceError> {
    sea_orm::sqlx::query(
        "INSERT INTO ws_v1_inode_delta
         (layer_id, ino, state, kind, size, mode, uid, gid, rdev, nlink,
          atime_ns, mtime_ns, ctime_ns, symlink_target, parent_hint,
          data_version, sequence)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(row.layer_id.as_bytes().as_slice())
    .bind(row.ino)
    .bind(row.state.discriminant() as i64)
    .bind(i64::from(row.kind))
    .bind(to_i64(row.size, "inode size")?)
    .bind(i64::from(row.mode))
    .bind(i64::from(row.uid))
    .bind(i64::from(row.gid))
    .bind(i64::from(row.rdev))
    .bind(i64::from(row.nlink))
    .bind(row.atime_ns)
    .bind(row.mtime_ns)
    .bind(row.ctime_ns)
    .bind(row.symlink_target.as_deref())
    .bind(row.parent_hint)
    .bind(to_i64(row.data_version, "data version")?)
    .bind(to_i64(row.sequence, "inode sequence")?)
    .execute(&mut **tx)
    .await
    .map_err(backend)?;
    Ok(())
}

async fn upsert_inode(
    tx: &mut Transaction<'_, Sqlite>,
    row: &InodeDelta,
) -> Result<(), WorkspaceError> {
    sea_orm::sqlx::query(
        "INSERT INTO ws_v1_inode_delta
         (layer_id, ino, state, kind, size, mode, uid, gid, rdev, nlink,
          atime_ns, mtime_ns, ctime_ns, symlink_target, parent_hint,
          data_version, sequence)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(layer_id, ino) DO UPDATE SET
             state = excluded.state, kind = excluded.kind, size = excluded.size,
             mode = excluded.mode, uid = excluded.uid, gid = excluded.gid,
             rdev = excluded.rdev, nlink = excluded.nlink,
             atime_ns = excluded.atime_ns, mtime_ns = excluded.mtime_ns,
             ctime_ns = excluded.ctime_ns, symlink_target = excluded.symlink_target,
             parent_hint = excluded.parent_hint, data_version = excluded.data_version,
             sequence = excluded.sequence",
    )
    .bind(row.layer_id.as_bytes().as_slice())
    .bind(row.ino)
    .bind(row.state.discriminant() as i64)
    .bind(i64::from(row.kind))
    .bind(to_i64(row.size, "inode size")?)
    .bind(i64::from(row.mode))
    .bind(i64::from(row.uid))
    .bind(i64::from(row.gid))
    .bind(i64::from(row.rdev))
    .bind(i64::from(row.nlink))
    .bind(row.atime_ns)
    .bind(row.mtime_ns)
    .bind(row.ctime_ns)
    .bind(row.symlink_target.as_deref())
    .bind(row.parent_hint)
    .bind(to_i64(row.data_version, "data version")?)
    .bind(to_i64(row.sequence, "inode sequence")?)
    .execute(&mut **tx)
    .await
    .map_err(backend)?;
    Ok(())
}

async fn insert_extent(
    tx: &mut Transaction<'_, Sqlite>,
    row: &DataExtentDelta,
) -> Result<(), WorkspaceError> {
    let (slice_id, slice_offset) = match row.kind {
        ExtentKind::Data {
            slice_id,
            slice_offset,
        } => (
            Some(to_i64(slice_id, "slice ID")?),
            Some(to_i64(slice_offset, "slice offset")?),
        ),
        ExtentKind::Hole => (None, None),
    };
    sea_orm::sqlx::query(
        "INSERT INTO ws_v1_data_extent_delta
         (layer_id, ino, chunk_index, logical_offset, length, kind,
          slice_id, slice_offset, sequence)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(row.layer_id.as_bytes().as_slice())
    .bind(row.ino)
    .bind(to_i64(row.chunk_index, "chunk index")?)
    .bind(to_i64(row.logical_offset, "logical offset")?)
    .bind(to_i64(row.length, "extent length")?)
    .bind(row.kind.discriminant() as i64)
    .bind(slice_id)
    .bind(slice_offset)
    .bind(to_i64(row.sequence, "extent sequence")?)
    .execute(&mut **tx)
    .await
    .map_err(backend)?;
    Ok(())
}

async fn upsert_xattr(
    tx: &mut Transaction<'_, Sqlite>,
    row: &XattrDelta,
) -> Result<(), WorkspaceError> {
    sea_orm::sqlx::query(
        "INSERT INTO ws_v1_xattr_delta(layer_id, ino, name, op, value, sequence)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(layer_id, ino, name) DO UPDATE SET
             op = excluded.op, value = excluded.value, sequence = excluded.sequence",
    )
    .bind(row.layer_id.as_bytes().as_slice())
    .bind(row.ino)
    .bind(row.name.as_slice())
    .bind(row.op.discriminant() as i64)
    .bind(row.value.as_deref())
    .bind(to_i64(row.sequence, "xattr sequence")?)
    .execute(&mut **tx)
    .await
    .map_err(backend)?;
    Ok(())
}

async fn upsert_acl(
    tx: &mut Transaction<'_, Sqlite>,
    row: &AclDelta,
) -> Result<(), WorkspaceError> {
    sea_orm::sqlx::query(
        "INSERT INTO ws_v1_acl_delta
         (layer_id, ino, acl_type, acl_id, op, value, sequence)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(layer_id, ino, acl_type, acl_id) DO UPDATE SET
             op = excluded.op, value = excluded.value, sequence = excluded.sequence",
    )
    .bind(row.layer_id.as_bytes().as_slice())
    .bind(row.ino)
    .bind(i64::from(row.acl_type))
    .bind(row.acl_id)
    .bind(row.op.discriminant() as i64)
    .bind(row.value.as_deref())
    .bind(to_i64(row.sequence, "ACL sequence")?)
    .execute(&mut **tx)
    .await
    .map_err(backend)?;
    Ok(())
}

fn decode_volume_header(row: SqliteRow) -> Result<VolumeHeader, WorkspaceError> {
    let schema_version = to_u32(row.try_get("schema_version").map_err(backend)?, "schema")?;
    if schema_version != WORKSPACE_SCHEMA_VERSION {
        return Err(WorkspaceError::UnsupportedSchemaVersion(schema_version));
    }
    let volume_format: String = row.try_get("volume_format").map_err(backend)?;
    if volume_format != VOLUME_FORMAT {
        return Err(WorkspaceError::UnsupportedVolumeFormat(volume_format));
    }
    Ok(VolumeHeader {
        volume_format,
        schema_version,
        volume_id: uuid_from_blob(row.try_get("volume_id").map_err(backend)?, "volume ID")?,
        created_at_ns: row.try_get("created_at_ns").map_err(backend)?,
    })
}

fn decode_workspace(row: &SqliteRow) -> Result<WorkspaceRecord, WorkspaceError> {
    let base_layer: Option<Vec<u8>> = row.try_get("fork_base_layer_id").map_err(backend)?;
    let base_version: Option<i64> = row.try_get("fork_base_version").map_err(backend)?;
    let base_hash: Option<Vec<u8>> = row.try_get("fork_base_root_hash").map_err(backend)?;
    let fork_base = match (base_layer, base_version, base_hash) {
        (None, None, None) => None,
        (Some(layer), Some(version), Some(hash)) => Some(BaseRevision {
            layer_id: LayerId::from_uuid(uuid_from_blob(layer, "fork base layer")?),
            sealed_version: to_u64(version, "fork base version")?,
            root_hash: hash_from_blob(hash, "fork base root hash")?,
        }),
        _ => {
            return Err(WorkspaceError::CorruptMetadata(
                "partial fork base revision".into(),
            ));
        }
    };
    Ok(WorkspaceRecord {
        workspace_id: WorkspaceId::from_uuid(uuid_from_blob(
            row.try_get("workspace_id").map_err(backend)?,
            "workspace ID",
        )?),
        head_layer_id: LayerId::from_uuid(uuid_from_blob(
            row.try_get("head_layer_id").map_err(backend)?,
            "head layer ID",
        )?),
        head_epoch: to_u64(row.try_get("head_epoch").map_err(backend)?, "head epoch")?,
        fork_base,
        owner_id: row.try_get("owner_id").map_err(backend)?,
        state: WorkspaceState::try_from(to_u8(
            row.try_get("state").map_err(backend)?,
            "workspace state",
        )?)?,
        created_at_ns: row.try_get("created_at_ns").map_err(backend)?,
        updated_at_ns: row.try_get("updated_at_ns").map_err(backend)?,
    })
}

fn decode_snapshot(row: &SqliteRow) -> Result<SnapshotRecord, WorkspaceError> {
    Ok(SnapshotRecord {
        snapshot_id: SnapshotId::from_uuid(uuid_from_blob(
            row.try_get("snapshot_id").map_err(backend)?,
            "snapshot ID",
        )?),
        name: row.try_get("name").map_err(backend)?,
        revision: BaseRevision {
            layer_id: LayerId::from_uuid(uuid_from_blob(
                row.try_get("layer_id").map_err(backend)?,
                "snapshot layer ID",
            )?),
            sealed_version: to_u64(
                row.try_get("sealed_version").map_err(backend)?,
                "snapshot sealed version",
            )?,
            root_hash: hash_from_blob(
                row.try_get("root_hash").map_err(backend)?,
                "snapshot root hash",
            )?,
        },
        owner_id: row.try_get("owner_id").map_err(backend)?,
        created_at_ns: row.try_get("created_at_ns").map_err(backend)?,
    })
}

fn decode_seal_journal(row: &SqliteRow) -> Result<SealJournal, WorkspaceError> {
    Ok(SealJournal {
        journal_id: JournalId::from_uuid(uuid_from_blob(
            row.try_get("journal_id").map_err(backend)?,
            "journal ID",
        )?),
        workspace_id: WorkspaceId::from_uuid(uuid_from_blob(
            row.try_get("workspace_id").map_err(backend)?,
            "journal workspace ID",
        )?),
        old_head_layer_id: LayerId::from_uuid(uuid_from_blob(
            row.try_get("old_head_layer_id").map_err(backend)?,
            "journal old head",
        )?),
        expected_head_epoch: to_u64(
            row.try_get("expected_head_epoch").map_err(backend)?,
            "journal head epoch",
        )?,
        phase: SealPhase::try_from(to_u8(row.try_get("phase").map_err(backend)?, "seal phase")?)?,
        pending_bytes: to_u64(
            row.try_get("pending_bytes").map_err(backend)?,
            "pending bytes",
        )?,
        delta_digest: row
            .try_get::<Option<Vec<u8>>, _>("delta_digest")
            .map_err(backend)?
            .map(|bytes| hash_from_blob(bytes, "journal delta digest"))
            .transpose()?,
        root_hash: row
            .try_get::<Option<Vec<u8>>, _>("root_hash")
            .map_err(backend)?
            .map(|bytes| hash_from_blob(bytes, "journal root hash"))
            .transpose()?,
        new_head_layer_id: row
            .try_get::<Option<Vec<u8>>, _>("new_head_layer_id")
            .map_err(backend)?
            .map(|bytes| uuid_from_blob(bytes, "journal new head").map(LayerId::from_uuid))
            .transpose()?,
        last_error: row.try_get("last_error").map_err(backend)?,
        created_at_ns: row.try_get("created_at_ns").map_err(backend)?,
        updated_at_ns: row.try_get("updated_at_ns").map_err(backend)?,
    })
}

fn decode_layer(row: &SqliteRow) -> Result<LayerRecord, WorkspaceError> {
    let schema_version = to_u32(row.try_get("schema_version").map_err(backend)?, "schema")?;
    if schema_version != WORKSPACE_SCHEMA_VERSION {
        return Err(WorkspaceError::UnsupportedSchemaVersion(schema_version));
    }
    Ok(LayerRecord {
        layer_id: LayerId::from_uuid(uuid_from_blob(
            row.try_get("layer_id").map_err(backend)?,
            "layer ID",
        )?),
        parent_layer_id: row
            .try_get::<Option<Vec<u8>>, _>("parent_layer_id")
            .map_err(backend)?
            .map(|bytes| uuid_from_blob(bytes, "parent layer ID").map(LayerId::from_uuid))
            .transpose()?,
        state: LayerState::try_from(to_u8(
            row.try_get("state").map_err(backend)?,
            "layer state",
        )?)?,
        schema_version,
        sealed_version: row
            .try_get::<Option<i64>, _>("sealed_version")
            .map_err(backend)?
            .map(|value| to_u64(value, "sealed version"))
            .transpose()?,
        delta_digest: row
            .try_get::<Option<Vec<u8>>, _>("delta_digest")
            .map_err(backend)?
            .map(|bytes| hash_from_blob(bytes, "delta digest"))
            .transpose()?,
        root_hash: row
            .try_get::<Option<Vec<u8>>, _>("root_hash")
            .map_err(backend)?
            .map(|bytes| hash_from_blob(bytes, "root hash"))
            .transpose()?,
        depth: to_u32(row.try_get("depth").map_err(backend)?, "layer depth")?,
        owner_workspace_id: row
            .try_get::<Option<Vec<u8>>, _>("owner_workspace_id")
            .map_err(backend)?
            .map(|bytes| uuid_from_blob(bytes, "owner workspace ID").map(WorkspaceId::from_uuid))
            .transpose()?,
        next_sequence: to_u64(
            row.try_get("next_sequence").map_err(backend)?,
            "next sequence",
        )?,
        owned_slice_count: to_u64(
            row.try_get("owned_slice_count").map_err(backend)?,
            "owned slice count",
        )?,
        owned_bytes: to_u64(row.try_get("owned_bytes").map_err(backend)?, "owned bytes")?,
        created_at_ns: row.try_get("created_at_ns").map_err(backend)?,
        sealed_at_ns: row.try_get("sealed_at_ns").map_err(backend)?,
    })
}

fn decode_dentry(row: SqliteRow) -> Result<DentryDelta, WorkspaceError> {
    let delta = DentryDelta {
        layer_id: LayerId::from_uuid(uuid_from_blob(
            row.try_get("layer_id").map_err(backend)?,
            "dentry layer ID",
        )?),
        parent_ino: row.try_get("parent_ino").map_err(backend)?,
        name: row.try_get("name").map_err(backend)?,
        op: DentryOp::try_from(to_u8(row.try_get("op").map_err(backend)?, "dentry op")?)?,
        ino: row.try_get("ino").map_err(backend)?,
        entry_type: row
            .try_get::<Option<i64>, _>("entry_type")
            .map_err(backend)?
            .map(|value| to_u8(value, "entry type"))
            .transpose()?,
        sequence: to_u64(row.try_get("sequence").map_err(backend)?, "sequence")?,
    };
    delta.validate()?;
    Ok(delta)
}

fn decode_inode(row: &SqliteRow) -> Result<InodeDelta, WorkspaceError> {
    Ok(InodeDelta {
        layer_id: LayerId::from_uuid(uuid_from_blob(
            row.try_get("layer_id").map_err(backend)?,
            "inode layer ID",
        )?),
        ino: row.try_get("ino").map_err(backend)?,
        state: InodeState::try_from(to_u8(
            row.try_get("state").map_err(backend)?,
            "inode state",
        )?)?,
        kind: to_u8(row.try_get("kind").map_err(backend)?, "inode kind")?,
        size: to_u64(row.try_get("size").map_err(backend)?, "inode size")?,
        mode: to_u32(row.try_get("mode").map_err(backend)?, "inode mode")?,
        uid: to_u32(row.try_get("uid").map_err(backend)?, "inode uid")?,
        gid: to_u32(row.try_get("gid").map_err(backend)?, "inode gid")?,
        rdev: to_u32(row.try_get("rdev").map_err(backend)?, "inode rdev")?,
        nlink: to_u32(row.try_get("nlink").map_err(backend)?, "inode nlink")?,
        atime_ns: row.try_get("atime_ns").map_err(backend)?,
        mtime_ns: row.try_get("mtime_ns").map_err(backend)?,
        ctime_ns: row.try_get("ctime_ns").map_err(backend)?,
        symlink_target: row.try_get("symlink_target").map_err(backend)?,
        parent_hint: row.try_get("parent_hint").map_err(backend)?,
        data_version: to_u64(
            row.try_get("data_version").map_err(backend)?,
            "data version",
        )?,
        sequence: to_u64(row.try_get("sequence").map_err(backend)?, "sequence")?,
    })
}

fn decode_extent(row: &SqliteRow) -> Result<DataExtentDelta, WorkspaceError> {
    let kind = match to_u8(row.try_get("kind").map_err(backend)?, "extent kind")? {
        0 => ExtentKind::Data {
            slice_id: to_u64(
                row.try_get::<Option<i64>, _>("slice_id")
                    .map_err(backend)?
                    .ok_or_else(|| {
                        WorkspaceError::CorruptMetadata("data extent without slice ID".into())
                    })?,
                "slice ID",
            )?,
            slice_offset: to_u64(
                row.try_get::<Option<i64>, _>("slice_offset")
                    .map_err(backend)?
                    .ok_or_else(|| {
                        WorkspaceError::CorruptMetadata("data extent without slice offset".into())
                    })?,
                "slice offset",
            )?,
        },
        1 => {
            let slice_id: Option<i64> = row.try_get("slice_id").map_err(backend)?;
            let slice_offset: Option<i64> = row.try_get("slice_offset").map_err(backend)?;
            if slice_id.is_some() || slice_offset.is_some() {
                return Err(WorkspaceError::CorruptMetadata(
                    "hole extent carries slice fields".into(),
                ));
            }
            ExtentKind::Hole
        }
        unknown => {
            return Err(WorkspaceError::CorruptMetadata(format!(
                "unknown extent kind {unknown}"
            )));
        }
    };
    let extent = DataExtentDelta {
        layer_id: LayerId::from_uuid(uuid_from_blob(
            row.try_get("layer_id").map_err(backend)?,
            "extent layer ID",
        )?),
        ino: row.try_get("ino").map_err(backend)?,
        chunk_index: to_u64(row.try_get("chunk_index").map_err(backend)?, "chunk index")?,
        logical_offset: to_u64(
            row.try_get("logical_offset").map_err(backend)?,
            "logical offset",
        )?,
        length: to_u64(row.try_get("length").map_err(backend)?, "extent length")?,
        kind,
        sequence: to_u64(row.try_get("sequence").map_err(backend)?, "sequence")?,
    };
    extent.validate()?;
    Ok(extent)
}

fn decode_xattr(row: &SqliteRow) -> Result<XattrDelta, WorkspaceError> {
    let delta = XattrDelta {
        layer_id: LayerId::from_uuid(uuid_from_blob(
            row.try_get("layer_id").map_err(backend)?,
            "xattr layer ID",
        )?),
        ino: row.try_get("ino").map_err(backend)?,
        name: row.try_get("name").map_err(backend)?,
        op: ValueOp::try_from(to_u8(row.try_get("op").map_err(backend)?, "xattr op")?)?,
        value: row.try_get("value").map_err(backend)?,
        sequence: to_u64(row.try_get("sequence").map_err(backend)?, "sequence")?,
    };
    validate_value(delta.op, delta.value.as_deref(), "xattr")?;
    Ok(delta)
}

fn decode_acl(row: &SqliteRow) -> Result<AclDelta, WorkspaceError> {
    let delta = AclDelta {
        layer_id: LayerId::from_uuid(uuid_from_blob(
            row.try_get("layer_id").map_err(backend)?,
            "ACL layer ID",
        )?),
        ino: row.try_get("ino").map_err(backend)?,
        acl_type: to_u8(row.try_get("acl_type").map_err(backend)?, "ACL type")?,
        acl_id: row.try_get("acl_id").map_err(backend)?,
        op: ValueOp::try_from(to_u8(row.try_get("op").map_err(backend)?, "ACL op")?)?,
        value: row.try_get("value").map_err(backend)?,
        sequence: to_u64(row.try_get("sequence").map_err(backend)?, "sequence")?,
    };
    validate_value(delta.op, delta.value.as_deref(), "ACL")?;
    Ok(delta)
}

fn decode_lease(row: &SqliteRow) -> Result<SnapshotLease, WorkspaceError> {
    let writable = match row.try_get::<i64, _>("writable").map_err(backend)? {
        0 => false,
        1 => true,
        value => {
            return Err(WorkspaceError::CorruptMetadata(format!(
                "invalid lease writable value {value}"
            )));
        }
    };
    Ok(SnapshotLease {
        lease_id: crate::workspace_overlay::ids::LeaseId::from_uuid(uuid_from_blob(
            row.try_get("lease_id").map_err(backend)?,
            "lease ID",
        )?),
        workspace_id: WorkspaceId::from_uuid(uuid_from_blob(
            row.try_get("workspace_id").map_err(backend)?,
            "lease workspace ID",
        )?),
        base_revision: BaseRevision {
            layer_id: LayerId::from_uuid(uuid_from_blob(
                row.try_get("base_layer_id").map_err(backend)?,
                "lease base layer ID",
            )?),
            sealed_version: to_u64(
                row.try_get("base_version").map_err(backend)?,
                "lease base version",
            )?,
            root_hash: hash_from_blob(
                row.try_get("base_root_hash").map_err(backend)?,
                "lease base root hash",
            )?,
        },
        holder_generation: to_u64(
            row.try_get("holder_generation").map_err(backend)?,
            "holder generation",
        )?,
        writable,
        state: LeaseState::try_from(to_u8(
            row.try_get("state").map_err(backend)?,
            "lease state",
        )?)?,
        expires_at_ns: row.try_get("expires_at_ns").map_err(backend)?,
        created_at_ns: row.try_get("created_at_ns").map_err(backend)?,
        updated_at_ns: row.try_get("updated_at_ns").map_err(backend)?,
    })
}

fn validate_value(op: ValueOp, value: Option<&[u8]>, kind: &str) -> Result<(), WorkspaceError> {
    match (op, value) {
        (ValueOp::Put, Some(_)) | (ValueOp::Whiteout, None) => Ok(()),
        _ => Err(WorkspaceError::CorruptMetadata(format!(
            "{kind} op/payload mismatch"
        ))),
    }
}

fn now_ns() -> Result<i64, WorkspaceError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| WorkspaceError::Backend(format!("system clock before epoch: {error}")))?;
    i64::try_from(duration.as_nanos())
        .map_err(|_| WorkspaceError::Backend("system time exceeds SQLite range".into()))
}

fn to_i64(value: u64, field: &str) -> Result<i64, WorkspaceError> {
    i64::try_from(value)
        .map_err(|_| WorkspaceError::CorruptMetadata(format!("{field} exceeds SQLite range")))
}

fn to_u64(value: i64, field: &str) -> Result<u64, WorkspaceError> {
    u64::try_from(value).map_err(|_| WorkspaceError::CorruptMetadata(format!("negative {field}")))
}

fn to_u32(value: i64, field: &str) -> Result<u32, WorkspaceError> {
    u32::try_from(value).map_err(|_| WorkspaceError::CorruptMetadata(format!("invalid {field}")))
}

fn to_u8(value: i64, field: &str) -> Result<u8, WorkspaceError> {
    u8::try_from(value).map_err(|_| WorkspaceError::CorruptMetadata(format!("invalid {field}")))
}

fn uuid_from_blob(bytes: Vec<u8>, field: &str) -> Result<Uuid, WorkspaceError> {
    Uuid::from_slice(&bytes)
        .map_err(|error| WorkspaceError::CorruptMetadata(format!("invalid {field}: {error}")))
}

fn hash_from_blob(bytes: Vec<u8>, field: &str) -> Result<[u8; 32], WorkspaceError> {
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        WorkspaceError::CorruptMetadata(format!(
            "invalid {field} length {}, expected 32",
            bytes.len()
        ))
    })
}

fn is_unique_violation(error: &sea_orm::sqlx::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("unique constraint") || message.contains("primary key")
}

fn backend(error: impl std::fmt::Display) -> WorkspaceError {
    WorkspaceError::Backend(error.to_string())
}
