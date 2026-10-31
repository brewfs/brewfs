//! SQLite implementation of the workspace catalog.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};

use async_trait::async_trait;
use sea_orm::sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow,
};
use sea_orm::sqlx::{Row, Sqlite, SqlitePool, Transaction};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::workspace_overlay::catalog::{
    AclMutation, AclQuery, AcquireLease, AppendDataExtent, CreateSnapshot, CreateVolumeRoot,
    CreateWorkspace, DataMutation, DataMutationResult, DeleteLayerMetadata, DentryQuery,
    ExtentQuery, GcSnapshot, HeadGuard, InodeMutation, InodeQuery, MarkDeleting, MutationResult,
    NamespaceMutation, PermissionMutation, PermissionSnapshot, PermissionSnapshotQuery,
    RecordOrphanSlice, ReleaseLease, RenewLease, SliceReference, VersionedMutation, WorkspaceStore,
    WorkspaceStoreCapabilities, XattrMutation, XattrQuery,
};
#[cfg(test)]
use crate::workspace_overlay::catalog::{InstallTestBase, TestBaseResult};
use crate::workspace_overlay::digest::{CanonicalLayerDelta, delta_digest, root_hash};
use crate::workspace_overlay::error::WorkspaceError;
use crate::workspace_overlay::ids::{JournalId, LayerId, LeaseId, SnapshotId, WorkspaceId};
use crate::workspace_overlay::model::{
    AclDelta, BaseRevision, DataExtentDelta, DentryDelta, DentryOp, ExtentKind, InodeDelta,
    InodeState, LayerRecord, LayerState, LeaseState, SealJournal, SealPhase, SnapshotLease,
    SnapshotRecord, ValueOp, VolumeHeader, WORKSPACE_SCHEMA_VERSION, WorkspaceRecord,
    WorkspaceState, XattrDelta,
};
use crate::workspace_overlay::publish::binding::{
    InstallPackedLowerBinding, PackedLowerBindingRecord, PublishPackedLowerBinding,
};
use crate::workspace_overlay::resolver::validate_layer_chain;

const VOLUME_FORMAT: &str = "workspace-v1";

/// State persisted by the v3 SQLite open sidecar. This record governs only
/// the sidecar APIs; it is not yet a mount or seal-recovery ownership gate.
/// Ordinary snapshot leases remain the mutation fence used by data writes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i64)]
pub enum V3OpenState {
    Recovering = 0,
    Ready = 1,
}

impl TryFrom<i64> for V3OpenState {
    type Error = WorkspaceError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Recovering),
            1 => Ok(Self::Ready),
            other => Err(WorkspaceError::CorruptMetadata(format!(
                "unknown v3 open state {other}"
            ))),
        }
    }
}

/// A fencing token returned by [`SqliteWorkspaceStore::open_workspace_v3`].
/// Every sidecar mutation must present the owner and generation returned by
/// open. A later takeover fences old tokens in the sidecar APIs only; mount,
/// mutation and seal recovery have not yet been connected to this token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V3OpenToken {
    pub workspace_id: WorkspaceId,
    pub owner_id: String,
    pub generation: u64,
    pub expires_at_ns: i64,
    pub state: V3OpenState,
    pub recovery_required: bool,
}

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
    upper_bucket TEXT,
    head_layer_id BLOB NOT NULL,
    head_epoch INTEGER NOT NULL,
    fork_base_layer_id BLOB,
    fork_base_version INTEGER,
    fork_base_root_hash BLOB,
    owner_id TEXT,
    state INTEGER NOT NULL,
    active_lease BLOB,
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
CREATE TABLE IF NOT EXISTS ws_v3_packed_bindings (
    workspace_id BLOB NOT NULL,
    binding_version INTEGER NOT NULL CHECK(binding_version > 0),
    record BLOB NOT NULL CHECK(length(record) <= 8192),
    PRIMARY KEY(workspace_id, binding_version)
);
CREATE TABLE IF NOT EXISTS ws_v3_packed_current (
    workspace_id BLOB PRIMARY KEY,
    binding_version INTEGER NOT NULL,
    FOREIGN KEY(workspace_id, binding_version)
        REFERENCES ws_v3_packed_bindings(workspace_id, binding_version)
);
CREATE TABLE IF NOT EXISTS ws_v3_workspace_open (
    workspace_id BLOB PRIMARY KEY,
    owner_id TEXT NOT NULL CHECK(length(CAST(owner_id AS BLOB)) BETWEEN 1 AND 256),
    generation INTEGER NOT NULL CHECK(generation > 0),
    expires_at_ns INTEGER NOT NULL,
    state INTEGER NOT NULL CHECK(state IN (0, 1)),
    recovery_required INTEGER NOT NULL CHECK(recovery_required IN (0, 1)),
    opened_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL,
    FOREIGN KEY(workspace_id) REFERENCES ws_v1_workspaces(workspace_id)
);
"#;

// Required columns for the Manager's read-only admission probe. Keep in sync
// with SCHEMA; the table names also bound empty-namespace initialization.
const MANAGER_CATALOG_COLUMNS: &[(&str, &str)] = &[
    (
        "ws_v1_volume_header",
        "singleton_id, volume_format, schema_version, volume_id, created_at_ns",
    ),
    (
        "ws_v1_workspaces",
        "workspace_id, head_layer_id, head_epoch, fork_base_layer_id, fork_base_version, fork_base_root_hash, owner_id, state, active_lease, created_at_ns, updated_at_ns",
    ),
    (
        "ws_v1_layers",
        "layer_id, parent_layer_id, state, schema_version, sealed_version, delta_digest, root_hash, depth, owner_workspace_id, next_sequence, owned_slice_count, owned_bytes, created_at_ns, sealed_at_ns",
    ),
    (
        "ws_v1_dentry_delta",
        "layer_id, parent_ino, name, op, ino, entry_type, sequence",
    ),
    (
        "ws_v1_inode_delta",
        "layer_id, ino, state, kind, size, mode, uid, gid, rdev, nlink, atime_ns, mtime_ns, ctime_ns, symlink_target, parent_hint, data_version, sequence",
    ),
    (
        "ws_v1_xattr_delta",
        "layer_id, ino, name, op, value, sequence",
    ),
    (
        "ws_v1_acl_delta",
        "layer_id, ino, acl_type, acl_id, op, value, sequence",
    ),
    (
        "ws_v1_data_extent_delta",
        "layer_id, ino, chunk_index, logical_offset, length, kind, slice_id, slice_offset, sequence",
    ),
    (
        "ws_v1_snapshot_leases",
        "lease_id, workspace_id, base_layer_id, base_version, base_root_hash, holder_generation, writable, state, expires_at_ns, created_at_ns, updated_at_ns",
    ),
    (
        "ws_v1_snapshots",
        "snapshot_id, name, layer_id, sealed_version, root_hash, owner_id, created_at_ns",
    ),
    (
        "ws_v1_seal_journal",
        "journal_id, workspace_id, old_head_layer_id, expected_head_epoch, phase, pending_bytes, delta_digest, root_hash, new_head_layer_id, last_error, created_at_ns, updated_at_ns",
    ),
    ("ws_v1_allocators", "name, next_value"),
    (
        "ws_v3_packed_bindings",
        "workspace_id, binding_version, record",
    ),
    ("ws_v3_packed_current", "workspace_id, binding_version"),
    (
        "ws_v3_workspace_open",
        "workspace_id, owner_id, generation, expires_at_ns, state, recovery_required, opened_at_ns, updated_at_ns",
    ),
];

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum StoreFailpoint {
    Disabled = 0,
    BeforeCommit = 1,
    AfterPublicationCommit = 2,
}

pub struct SqliteWorkspaceStore {
    pool: SqlitePool,
    write_gate: Arc<Mutex<()>>,
    #[cfg(test)]
    failpoint: AtomicU8,
    #[cfg(test)]
    sidecar_clock_ns: AtomicI64,
    #[cfg(test)]
    sidecar_commit_clock_ns: AtomicI64,
}

impl SqliteWorkspaceStore {
    /// Reject packed sidecars at the Manager boundary even if their workspace
    /// row or initial history is missing. Absence of version 1 is not evidence
    /// that a partially restored catalog is native-only.
    pub async fn has_packed_catalog_bindings(&self) -> Result<bool, WorkspaceError> {
        let present: i64 = sea_orm::sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM ws_v3_packed_bindings) OR EXISTS(SELECT 1 FROM ws_v3_packed_current)",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;
        Ok(present != 0)
    }

    /// Check the schema used by Manager without creating or migrating tables.
    /// A header alone cannot establish that an imported catalog is usable.
    pub async fn validate_manager_catalog_schema(&self) -> Result<(), WorkspaceError> {
        let actual_tables = self.schema_table_names().await?;
        for (table, columns) in MANAGER_CATALOG_COLUMNS {
            if !actual_tables.iter().any(|name| name.as_str() == *table) {
                return Err(WorkspaceError::UnsupportedCapability(
                    "complete workspace-v1 SQLite catalog schema",
                ));
            }
            // Both identifier lists are compile-time constants, not request data.
            let probe = format!("SELECT {columns} FROM \"{table}\" LIMIT 0");
            sea_orm::sqlx::query(&probe)
                .execute(&self.pool)
                .await
                .map_err(|error| match &error {
                    sea_orm::sqlx::Error::Database(database)
                        if database.code().as_deref() == Some("1") =>
                    {
                        WorkspaceError::UnsupportedCapability(
                            "complete workspace-v1 SQLite catalog schema",
                        )
                    }
                    _ => backend(error),
                })?;
        }
        Ok(())
    }

    /// Initialize a Manager-owned catalog only in an empty namespace.
    pub async fn create_volume_root_in_empty_namespace(
        &self,
        request: CreateVolumeRoot,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        self.create_volume_root_inner(request, true).await
    }

    async fn create_volume_root_inner(
        &self,
        request: CreateVolumeRoot,
        require_empty: bool,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        if request.volume_format != VOLUME_FORMAT
            || request.schema_version != WORKSPACE_SCHEMA_VERSION
        {
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
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        if require_empty {
            Self::require_empty_namespace(&mut tx).await?;
            // SCHEMA contains only CREATE statements with no quoted semicolons
            // or trigger bodies. Execute each on this same reserved-write tx.
            for statement in SCHEMA.split(';').filter(|sql| !sql.trim().is_empty()) {
                sea_orm::sqlx::query(statement)
                    .execute(&mut *tx)
                    .await
                    .map_err(backend)?;
            }
        }
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
             (workspace_id, upper_bucket, head_layer_id, head_epoch,
              fork_base_layer_id, fork_base_version, fork_base_root_hash,
              owner_id, state, created_at_ns, updated_at_ns)
             VALUES (?, ?, ?, 0, ?, 1, ?, ?, ?, ?, ?)",
        )
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(request.upper_bucket.as_deref())
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
            upper_bucket: request.upper_bucket,
            head_layer_id: request.writable_layer_id,
            head_epoch: 0,
            fork_base: Some(BaseRevision {
                layer_id: request.root_layer_id,
                sealed_version: 1,
                root_hash,
            }),
            owner_id: request.owner_id,
            state: WorkspaceState::Active,
            active_lease: None,
            created_at_ns: now,
            updated_at_ns: now,
        })
    }

    async fn require_empty_namespace(
        tx: &mut Transaction<'_, Sqlite>,
    ) -> Result<(), WorkspaceError> {
        // Keep this allowlist in sync with SCHEMA. No foreign tables, views,
        // triggers, or indexes may gain meaning merely by opening a Manager.
        const INDEXES: &[&str] = &[
            "ws_v1_layers_parent_idx",
            "ws_v1_dentry_parent_idx",
            "ws_v1_extent_lookup_idx",
            "ws_v1_one_writer_idx",
            "ws_v1_snapshot_name_idx",
        ];
        let not_empty = || {
            WorkspaceError::Conflict(crate::workspace_overlay::error::ConflictDetail {
                path: Vec::new(),
                reason: "volume initialization requires an empty catalog namespace".into(),
            })
        };
        let objects = sea_orm::sqlx::query(
            "SELECT type, name FROM sqlite_master WHERE substr(name, 1, 7) != 'sqlite_'",
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(backend)?;
        let mut present_tables = Vec::new();
        for object in objects {
            let kind: String = object.try_get("type").map_err(backend)?;
            let name: String = object.try_get("name").map_err(backend)?;
            let allowed = match kind.as_str() {
                "table" => MANAGER_CATALOG_COLUMNS
                    .iter()
                    .any(|(table, _)| *table == name),
                "index" => INDEXES.contains(&name.as_str()),
                _ => false,
            };
            if !allowed {
                return Err(not_empty());
            }
            if kind == "table" {
                present_tables.push(name);
            }
        }
        for table in present_tables {
            // Identifiers come only from the compile-time allowlist above.
            let occupied: i64 = sea_orm::sqlx::query_scalar(&format!(
                "SELECT EXISTS(SELECT 1 FROM \"{table}\" LIMIT 1)"
            ))
            .fetch_one(&mut **tx)
            .await
            .map_err(backend)?;
            if occupied != 0 {
                return Err(not_empty());
            }
        }
        Ok(())
    }

    pub async fn connect(url: &str) -> Result<Self, WorkspaceError> {
        Self::connect_with_creation(url, true).await
    }

    /// Open an existing catalog without recreating a file removed between a
    /// caller's identity/existence check and SQLite opening the database.
    pub async fn connect_existing(url: &str) -> Result<Self, WorkspaceError> {
        Self::connect_with_creation(url, false).await
    }

    async fn connect_with_creation(
        url: &str,
        create_if_missing: bool,
    ) -> Result<Self, WorkspaceError> {
        let is_memory = url.contains("::memory:");
        let mut options = SqliteConnectOptions::from_str(url)
            .map_err(backend)?
            .create_if_missing(create_if_missing)
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
            #[cfg(test)]
            sidecar_clock_ns: AtomicI64::new(-1),
            #[cfg(test)]
            sidecar_commit_clock_ns: AtomicI64::new(-1),
        })
    }

    #[cfg(test)]
    pub(super) async fn rewrite_packed_binding_for_test(
        &self,
        workspace_id: WorkspaceId,
        record: &PackedLowerBindingRecord,
        drop_initial: bool,
    ) -> Result<(), WorkspaceError> {
        let bytes = record.encode()?;
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        sea_orm::sqlx::query(
            "INSERT OR REPLACE INTO ws_v3_packed_bindings(workspace_id, binding_version, record)
             VALUES (?, ?, ?)",
        )
        .bind(workspace_id.as_bytes().as_slice())
        .bind(to_i64(
            record.binding.binding_version,
            "packed binding version",
        )?)
        .bind(bytes)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sea_orm::sqlx::query(
            "UPDATE ws_v3_packed_current SET binding_version = ? WHERE workspace_id = ?",
        )
        .bind(to_i64(
            record.binding.binding_version,
            "packed binding version",
        )?)
        .bind(workspace_id.as_bytes().as_slice())
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        if drop_initial {
            sea_orm::sqlx::query(
                "DELETE FROM ws_v3_packed_bindings
                 WHERE workspace_id = ? AND binding_version = 1",
            )
            .bind(workspace_id.as_bytes().as_slice())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        tx.commit().await.map_err(backend)
    }

    fn sidecar_now_ns(&self) -> Result<i64, WorkspaceError> {
        #[cfg(test)]
        {
            let controlled = self.sidecar_clock_ns.load(Ordering::SeqCst);
            if controlled >= 0 {
                return Ok(controlled);
            }
        }
        now_ns()
    }

    fn sidecar_before_commit_ns(&self) -> Result<i64, WorkspaceError> {
        #[cfg(test)]
        {
            let advanced = self.sidecar_commit_clock_ns.swap(-1, Ordering::SeqCst);
            if advanced >= 0 {
                self.sidecar_clock_ns.store(advanced, Ordering::SeqCst);
            }
        }
        self.sidecar_now_ns()
    }

    /// Reject processing that crossed the sampled expiry after SQL staging.
    /// This is the last application check before requesting SQLite commit;
    /// SQLite does not atomically compare the wall clock with the eventual
    /// commit visibility point, which can occur after this check.
    fn recheck_sidecar_deadline(&self, deadline_ns: i64) -> Result<(), WorkspaceError> {
        if self.sidecar_before_commit_ns()? >= deadline_ns {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    async fn commit_sidecar_tx(
        &self,
        tx: Transaction<'_, Sqlite>,
        deadline_ns: i64,
    ) -> Result<(), WorkspaceError> {
        if let Err(error) = self.recheck_sidecar_deadline(deadline_ns) {
            tx.rollback().await.map_err(backend)?;
            return Err(error);
        }
        tx.commit().await.map_err(backend)
    }

    #[cfg(test)]
    pub(super) fn set_sidecar_time_ns(&self, now: i64) {
        assert!(now >= 0);
        self.sidecar_clock_ns.store(now, Ordering::SeqCst);
    }

    /// Advance only this store's sidecar clock after staging SQL writes and
    /// immediately before its next sidecar commit boundary. No sleeps or
    /// global clock state are shared between concurrently running tests.
    #[cfg(test)]
    pub(super) fn set_sidecar_commit_time_ns(&self, now: i64) {
        assert!(now >= 0);
        self.sidecar_commit_clock_ns.store(now, Ordering::SeqCst);
    }

    /// Claim the v3 open sidecar record for an already initialized workspace.
    ///
    /// The record is deliberately separate from the mutation lease.  It
    /// lets a new process take over after the old process's expiry. A takeover
    /// increments the persisted generation and fences delayed sidecar calls.
    /// Mount and seal-recovery ownership remain separate until they are wired
    /// to this token.
    pub async fn open_workspace_v3(
        &self,
        workspace_id: WorkspaceId,
        owner_id: impl Into<String>,
        ttl: Duration,
    ) -> Result<V3OpenToken, WorkspaceError> {
        let owner_id = owner_id.into();
        if owner_id.trim().is_empty() || owner_id.len() > 256 {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open owner id must contain 1..=256 bytes".into(),
            ));
        }
        let ttl_ns = duration_ns(ttl)?;
        if ttl_ns <= 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open ttl must be positive".into(),
            ));
        }

        let _writer = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        Self::validate_v3_schema_tx(&mut tx).await?;
        let recovery_required = Self::workspace_recovery_required_tx(&mut tx, workspace_id).await?;
        Self::validate_v3_open_view_tx(&mut tx, workspace_id, recovery_required).await?;

        let now = self.sidecar_now_ns()?;
        let expires_at_ns = now
            .checked_add(ttl_ns)
            .ok_or_else(|| WorkspaceError::Backend("v3 open expiry overflow".into()))?;
        let row = sea_orm::sqlx::query(
            "SELECT owner_id, generation, expires_at_ns, state, recovery_required,
                    opened_at_ns, updated_at_ns
             FROM ws_v3_workspace_open WHERE workspace_id = ?",
        )
        .bind(workspace_id.as_bytes().as_slice())
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?;

        let mut commit_deadline_ns = expires_at_ns;
        let token = if let Some(row) = row {
            let current = Self::decode_open_token(&row, workspace_id)?;
            if current.expires_at_ns > now && current.owner_id != owner_id {
                return Err(WorkspaceError::Busy);
            }
            let generation = if current.expires_at_ns > now {
                // A live-owner retry is also a renewal. It must finish before
                // both the old ownership expiry and the proposed new expiry.
                commit_deadline_ns = commit_deadline_ns.min(current.expires_at_ns);
                current.generation
            } else {
                current.generation.checked_add(1).ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("v3 open generation overflow".into())
                })?
            };
            let state = if recovery_required {
                V3OpenState::Recovering
            } else {
                V3OpenState::Ready
            };
            sea_orm::sqlx::query(
                "UPDATE ws_v3_workspace_open
                 SET owner_id = ?, generation = ?, expires_at_ns = ?, state = ?,
                     recovery_required = ?, updated_at_ns = ?
                 WHERE workspace_id = ?",
            )
            .bind(&owner_id)
            .bind(to_i64(generation, "v3 open generation")?)
            .bind(expires_at_ns)
            .bind(state as i64)
            .bind(if recovery_required { 1_i64 } else { 0_i64 })
            .bind(now)
            .bind(workspace_id.as_bytes().as_slice())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
            V3OpenToken {
                workspace_id,
                owner_id: owner_id.clone(),
                generation,
                expires_at_ns,
                state,
                recovery_required,
            }
        } else {
            let state = if recovery_required {
                V3OpenState::Recovering
            } else {
                V3OpenState::Ready
            };
            sea_orm::sqlx::query(
                "INSERT INTO ws_v3_workspace_open
                 (workspace_id, owner_id, generation, expires_at_ns, state,
                  recovery_required, opened_at_ns, updated_at_ns)
                 VALUES (?, ?, 1, ?, ?, ?, ?, ?)",
            )
            .bind(workspace_id.as_bytes().as_slice())
            .bind(&owner_id)
            .bind(expires_at_ns)
            .bind(state as i64)
            .bind(if recovery_required { 1_i64 } else { 0_i64 })
            .bind(now)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
            V3OpenToken {
                workspace_id,
                owner_id: owner_id.clone(),
                generation: 1,
                expires_at_ns,
                state,
                recovery_required,
            }
        };
        self.commit_sidecar_tx(tx, commit_deadline_ns).await?;
        Ok(token)
    }

    /// Mark a v3 open as ready after the caller has completed seal/binding
    /// recovery.  The check is performed in the same transaction as the state
    /// transition, so a new incomplete seal cannot be hidden by a stale owner.
    pub async fn mark_workspace_v3_ready(
        &self,
        token: &V3OpenToken,
    ) -> Result<V3OpenToken, WorkspaceError> {
        let _writer = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        Self::validate_v3_schema_tx(&mut tx).await?;
        let current = Self::load_open_token_tx(&mut tx, token.workspace_id).await?;
        Self::check_open_token(&current, token, self.sidecar_now_ns()?)?;
        if Self::workspace_recovery_required_tx(&mut tx, token.workspace_id).await? {
            return Err(WorkspaceError::Busy);
        }
        Self::validate_v3_open_view_tx(&mut tx, token.workspace_id, false).await?;
        let now = self.sidecar_now_ns()?;
        sea_orm::sqlx::query(
            "UPDATE ws_v3_workspace_open
             SET state = ?, recovery_required = 0, updated_at_ns = ?
             WHERE workspace_id = ? AND owner_id = ? AND generation = ?",
        )
        .bind(V3OpenState::Ready as i64)
        .bind(now)
        .bind(token.workspace_id.as_bytes().as_slice())
        .bind(&token.owner_id)
        .bind(to_i64(token.generation, "v3 open generation")?)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        let mut ready = current;
        ready.state = V3OpenState::Ready;
        ready.recovery_required = false;
        self.commit_sidecar_tx(tx, ready.expires_at_ns).await?;
        Ok(ready)
    }

    /// Extend an open token while preserving the same generation.
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
        let _writer = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let current = Self::load_open_token_tx(&mut tx, token.workspace_id).await?;
        let now = self.sidecar_now_ns()?;
        Self::check_open_token(&current, token, now)?;
        let expires_at_ns = now
            .checked_add(ttl_ns)
            .ok_or_else(|| WorkspaceError::Backend("v3 open expiry overflow".into()))?;
        let commit_deadline_ns = current.expires_at_ns.min(expires_at_ns);
        sea_orm::sqlx::query(
            "UPDATE ws_v3_workspace_open SET expires_at_ns = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND owner_id = ? AND generation = ?",
        )
        .bind(expires_at_ns)
        .bind(now)
        .bind(token.workspace_id.as_bytes().as_slice())
        .bind(&token.owner_id)
        .bind(to_i64(token.generation, "v3 open generation")?)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        let mut renewed = current;
        renewed.expires_at_ns = expires_at_ns;
        self.commit_sidecar_tx(tx, commit_deadline_ns).await?;
        Ok(renewed)
    }

    /// Release an open token without deleting its generation history.  The
    /// expiry timestamp is advanced to the current authoritative time so a
    /// subsequent owner must perform a generation takeover.
    pub async fn close_workspace_v3(&self, token: &V3OpenToken) -> Result<(), WorkspaceError> {
        let _writer = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let current = Self::load_open_token_tx(&mut tx, token.workspace_id).await?;
        Self::check_open_token(&current, token, self.sidecar_now_ns()?)?;
        let now = self.sidecar_now_ns()?;
        sea_orm::sqlx::query(
            "UPDATE ws_v3_workspace_open SET expires_at_ns = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND owner_id = ? AND generation = ?",
        )
        .bind(now)
        .bind(now)
        .bind(token.workspace_id.as_bytes().as_slice())
        .bind(&token.owner_id)
        .bind(to_i64(token.generation, "v3 open generation")?)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        self.commit_sidecar_tx(tx, current.expires_at_ns).await
    }

    async fn load_open_token_tx(
        tx: &mut Transaction<'_, Sqlite>,
        workspace_id: WorkspaceId,
    ) -> Result<V3OpenToken, WorkspaceError> {
        let row = sea_orm::sqlx::query(
            "SELECT owner_id, generation, expires_at_ns, state, recovery_required
             FROM ws_v3_workspace_open WHERE workspace_id = ?",
        )
        .bind(workspace_id.as_bytes().as_slice())
        .fetch_optional(&mut **tx)
        .await
        .map_err(backend)?
        .ok_or(WorkspaceError::WorkspaceNotFound(workspace_id))?;
        Self::decode_open_token(&row, workspace_id)
    }

    fn decode_open_token(
        row: &SqliteRow,
        workspace_id: WorkspaceId,
    ) -> Result<V3OpenToken, WorkspaceError> {
        let owner_id: String = row.try_get("owner_id").map_err(backend)?;
        if owner_id.trim().is_empty() || owner_id.len() > 256 {
            return Err(WorkspaceError::CorruptMetadata(
                "persisted v3 open owner id must contain 1..=256 bytes".into(),
            ));
        }
        let generation_raw: i64 = row.try_get("generation").map_err(backend)?;
        if generation_raw <= 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "persisted v3 open generation must be positive".into(),
            ));
        }
        let state_raw: i64 = row.try_get("state").map_err(backend)?;
        let state = V3OpenState::try_from(state_raw)?;
        let recovery_raw: i64 = row.try_get("recovery_required").map_err(backend)?;
        let recovery_required = match recovery_raw {
            0 => false,
            1 => true,
            _ => {
                return Err(WorkspaceError::CorruptMetadata(
                    "persisted v3 recovery flag must be 0 or 1".into(),
                ));
            }
        };
        if (state == V3OpenState::Recovering) != recovery_required {
            return Err(WorkspaceError::CorruptMetadata(
                "persisted v3 open state/recovery flag disagree".into(),
            ));
        }
        Ok(V3OpenToken {
            workspace_id,
            owner_id,
            generation: generation_raw as u64,
            expires_at_ns: row.try_get("expires_at_ns").map_err(backend)?,
            state,
            recovery_required,
        })
    }

    fn check_open_token(
        current: &V3OpenToken,
        expected: &V3OpenToken,
        now: i64,
    ) -> Result<(), WorkspaceError> {
        if current.owner_id != expected.owner_id
            || current.generation != expected.generation
            || current.expires_at_ns <= now
        {
            return Err(WorkspaceError::Fenced);
        }
        Ok(())
    }

    async fn workspace_recovery_required_tx(
        tx: &mut Transaction<'_, Sqlite>,
        workspace_id: WorkspaceId,
    ) -> Result<bool, WorkspaceError> {
        let incomplete: i64 = sea_orm::sqlx::query_scalar(
            "SELECT COUNT(*) FROM ws_v1_seal_journal
             WHERE workspace_id = ? AND phase NOT IN (?, ?)",
        )
        .bind(workspace_id.as_bytes().as_slice())
        .bind(SealPhase::Completed.discriminant() as i64)
        .bind(SealPhase::Aborted.discriminant() as i64)
        .fetch_one(&mut **tx)
        .await
        .map_err(backend)?;
        if incomplete != 0 {
            return Ok(true);
        }
        // Historical binding versions are normal. Only a current pointer
        // whose target is absent is dangling; the view validator also checks
        // record decoding and a claimed namespace with no current pointer.
        let dangling_binding: i64 = sea_orm::sqlx::query_scalar(
            "SELECT CASE WHEN EXISTS (
                 SELECT 1 FROM ws_v3_packed_current c
                 WHERE c.workspace_id = ?
                   AND NOT EXISTS (
                       SELECT 1 FROM ws_v3_packed_bindings b
                       WHERE b.workspace_id = c.workspace_id
                         AND b.binding_version = c.binding_version))
             THEN 1 ELSE 0 END",
        )
        .bind(workspace_id.as_bytes().as_slice())
        .fetch_one(&mut **tx)
        .await
        .map_err(backend)?;
        Ok(dangling_binding != 0)
    }

    /// Opening never initializes or repairs a catalog. A missing table in an
    /// already marked volume is corruption, not an empty table to recreate.
    /// This checks table presence, not complete column/index signatures; it
    /// provides no schema migration path. Open decodes the fields it consumes.
    async fn validate_v3_schema_tx(tx: &mut Transaction<'_, Sqlite>) -> Result<(), WorkspaceError> {
        for table in [
            "ws_v1_volume_header",
            "ws_v1_workspaces",
            "ws_v1_layers",
            "ws_v1_dentry_delta",
            "ws_v1_inode_delta",
            "ws_v1_xattr_delta",
            "ws_v1_acl_delta",
            "ws_v1_data_extent_delta",
            "ws_v1_snapshot_leases",
            "ws_v1_snapshots",
            "ws_v1_seal_journal",
            "ws_v1_allocators",
            "ws_v3_packed_bindings",
            "ws_v3_packed_current",
            "ws_v3_workspace_open",
        ] {
            let exists: i64 = sea_orm::sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)",
            )
            .bind(table)
            .fetch_one(&mut **tx)
            .await
            .map_err(backend)?;
            if exists == 0 {
                return Err(WorkspaceError::CorruptMetadata(format!(
                    "v3 open requires initialized table {table}"
                )));
            }
        }
        Ok(())
    }

    async fn validate_v3_open_view_tx(
        tx: &mut Transaction<'_, Sqlite>,
        workspace_id: WorkspaceId,
        recovery_required: bool,
    ) -> Result<(), WorkspaceError> {
        let header = sea_orm::sqlx::query(
            "SELECT volume_format, schema_version, volume_id, created_at_ns
             FROM ws_v1_volume_header WHERE singleton_id = 1",
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(backend)?
        .ok_or_else(|| {
            WorkspaceError::CorruptMetadata("v3 open requires an initialized volume header".into())
        })?;
        decode_volume_header(header)?;
        let workspace = load_workspace_tx(tx, workspace_id).await?;
        let expected_head_state = match workspace.state {
            WorkspaceState::Active => LayerState::Writable,
            WorkspaceState::Sealing if recovery_required => LayerState::Sealing,
            state => {
                return Err(WorkspaceError::CorruptMetadata(format!(
                    "v3 open rejects workspace state {state:?}"
                )));
            }
        };
        let head = permission_layer_tx(tx, workspace.head_layer_id).await?;
        if head.state != expected_head_state || head.owner_workspace_id != Some(workspace_id) {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open head state or workspace ownership mismatch".into(),
            ));
        }
        if head.sealed_version.is_some() || head.delta_digest.is_some() || head.root_hash.is_some()
        {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open mutable head contains sealed revision fields".into(),
            ));
        }

        // A journal can temporarily leave the workspace with a Sealing head
        // or a deeper chain before recovery compacts it. Validate that chain
        // within this transaction, and require the fixed pair for Ready.
        let mut validated_head = head.clone();
        validated_head.state = LayerState::Writable;
        let mut chain = vec![validated_head];
        let mut parent = head.parent_layer_id;
        while let Some(layer_id) = parent {
            if chain.len() >= crate::workspace_overlay::model::LAYER_CHAIN_HARD_LIMIT as usize {
                return Err(WorkspaceError::LayerDepthLimit {
                    depth: chain.len() as u32 + 1,
                    hard_limit: crate::workspace_overlay::model::LAYER_CHAIN_HARD_LIMIT,
                });
            }
            let layer = permission_layer_tx(tx, layer_id).await?;
            if layer.owner_workspace_id.is_some() {
                return Err(WorkspaceError::CorruptMetadata(
                    "v3 open sealed base has a workspace owner".into(),
                ));
            }
            parent = layer.parent_layer_id;
            chain.push(layer);
        }
        validate_layer_chain(workspace.head_layer_id, &chain)?;
        if chain.len() < 2 || (!recovery_required && chain.len() != 2) {
            return Err(WorkspaceError::CorruptMetadata(
                "v3 open requires a writable head and a flat sealed base before Ready".into(),
            ));
        }

        if let Some(record) = packed_binding_current_tx(tx, workspace_id).await? {
            let base = &chain[1];
            if !recovery_required
                && (record.head_layer_id != workspace.head_layer_id
                    || record.head_epoch != workspace.head_epoch
                    || record.base_revision.layer_id != base.layer_id
                    || Some(record.base_revision.sealed_version) != base.sealed_version
                    || Some(record.base_revision.root_hash) != base.root_hash)
            {
                return Err(WorkspaceError::CorruptMetadata(
                    "v3 open current PWB3 binding disagrees with head/base".into(),
                ));
            }
        }
        Ok(())
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

    /// 为已有目录增加 `active_lease`，并在同一事务内恢复当前租约指针。
    async fn migrate_workspace_columns(pool: &SqlitePool) -> Result<(), WorkspaceError> {
        let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.map_err(backend)?;
        let columns = sea_orm::sqlx::query("PRAGMA table_info(ws_v1_workspaces)")
            .fetch_all(&mut *tx)
            .await
            .map_err(backend)?;
        let names = columns
            .iter()
            .map(|row| row.try_get::<String, _>("name").map_err(backend))
            .collect::<Result<Vec<_>, _>>()?;
        let has_active_lease = names.iter().any(|name| name == "active_lease");
        if !names.iter().any(|name| name == "upper_bucket") {
            sea_orm::sqlx::query("ALTER TABLE ws_v1_workspaces ADD COLUMN upper_bucket TEXT")
                .execute(&mut *tx)
                .await
                .map_err(backend)?;
        }
        if !has_active_lease {
            sea_orm::sqlx::query("ALTER TABLE ws_v1_workspaces ADD COLUMN active_lease BLOB")
                .execute(&mut *tx)
                .await
                .map_err(backend)?;
            // 根据有效可写租约恢复旧目录的租约指针。
            let now = now_ns()?;
            sea_orm::sqlx::query(
                "UPDATE ws_v1_workspaces SET active_lease = (
                     SELECT l.lease_id FROM ws_v1_snapshot_leases l
                     WHERE l.workspace_id = ws_v1_workspaces.workspace_id
                       AND l.writable = 1 AND l.state = ? AND l.expires_at_ns > ?
                     ORDER BY l.updated_at_ns DESC, l.lease_id DESC
                     LIMIT 1
                 )",
            )
            .bind(LeaseState::Active.discriminant() as i64)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    pub async fn schema_table_names(&self) -> Result<Vec<String>, WorkspaceError> {
        let rows = sea_orm::sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND (name LIKE 'ws_v1_%' OR name LIKE 'ws_v3_%') ORDER BY name",
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
    async fn load_packed_lower_binding(
        &self,
        guard: HeadGuard,
    ) -> Result<Option<crate::workspace_overlay::catalog::PackedLowerBinding>, WorkspaceError> {
        Ok(self
            .load_packed_binding_record(guard)
            .await?
            .map(|record| record.binding))
    }

    async fn load_packed_binding_record(
        &self,
        guard: HeadGuard,
    ) -> Result<Option<PackedLowerBindingRecord>, WorkspaceError> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
        Self::checked_guard(&mut tx, &guard).await?;
        let record = packed_binding_current_tx(&mut tx, guard.workspace_id).await?;
        if let Some(record) = &record {
            let initial_bytes: Option<Vec<u8>> = sea_orm::sqlx::query_scalar(
                "SELECT record FROM ws_v3_packed_bindings
                 WHERE workspace_id = ? AND binding_version = 1",
            )
            .bind(guard.workspace_id.as_bytes().as_slice())
            .fetch_optional(&mut *tx)
            .await
            .map_err(backend)?;
            let initial =
                PackedLowerBindingRecord::decode(initial_bytes.as_deref().ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("PWB3 initial history is missing".into())
                })?)?;
            if initial.workspace_id != guard.workspace_id || initial.binding.binding_version != 1 {
                return Err(WorkspaceError::CorruptMetadata(
                    "PWB3 initial history key/record disagree".into(),
                ));
            }
            let head = permission_layer_tx(&mut tx, guard.expected_head_layer_id).await?;
            let base_id = head.parent_layer_id.ok_or(WorkspaceError::Fenced)?;
            let base = permission_layer_tx(&mut tx, base_id).await?;
            record.validate_for_guard(&guard, &base)?;
        }
        Self::checked_guard(&mut tx, &guard).await?;
        tx.commit().await.map_err(backend)?;
        Ok(record)
    }

    async fn load_packed_binding_version(
        &self,
        workspace_id: WorkspaceId,
        version: u64,
    ) -> Result<Option<PackedLowerBindingRecord>, WorkspaceError> {
        let bytes: Option<Vec<u8>> = sea_orm::sqlx::query_scalar("SELECT record FROM ws_v3_packed_bindings WHERE workspace_id = ? AND binding_version = ?")
            .bind(workspace_id.as_bytes().as_slice()).bind(to_i64(version, "packed binding version")?)
            .fetch_optional(&self.pool).await.map_err(backend)?;
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
        let record = request.record()?;
        let bytes = record.encode()?;
        let _writer = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        Self::checked_guard(&mut tx, &request.guard).await?;
        for expected in &request.expected_layers {
            if permission_layer_tx(&mut tx, expected.layer_id).await? != *expected {
                return Err(WorkspaceError::Busy);
            }
        }
        if load_revision_tx(&mut tx, request.expected_base.layer_id).await? != request.expected_base
        {
            return Err(WorkspaceError::Busy);
        }
        if packed_binding_current_tx(&mut tx, request.guard.workspace_id).await?
            != request.expected_binding
        {
            return Err(WorkspaceError::Busy);
        }
        let root =
            sea_orm::sqlx::query("SELECT * FROM ws_v1_inode_delta WHERE layer_id = ? AND ino = 1")
                .bind(request.expected_base.layer_id.as_bytes().as_slice())
                .fetch_optional(&mut *tx)
                .await
                .map_err(backend)?
                .ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("initial native base root missing".into())
                })?;
        request.validate_native_root(&decode_inode(&root)?)?;
        let allocator: Option<i64> = sea_orm::sqlx::query_scalar(
            "SELECT next_value FROM ws_v1_allocators WHERE name = 'inode'",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?;
        if allocator != Some(2) {
            return Err(WorkspaceError::UnsupportedCapability(
                "first packed binding requires unissued native inode IDs",
            ));
        }
        sea_orm::sqlx::query("INSERT INTO ws_v3_packed_bindings(workspace_id, binding_version, record) VALUES (?, ?, ?)")
            .bind(record.workspace_id.as_bytes().as_slice()).bind(to_i64(record.binding.binding_version, "packed binding version")?).bind(bytes)
            .execute(&mut *tx).await.map_err(backend)?;
        sea_orm::sqlx::query(
            "INSERT INTO ws_v3_packed_current(workspace_id, binding_version) VALUES (?, ?)",
        )
        .bind(record.workspace_id.as_bytes().as_slice())
        .bind(to_i64(
            record.binding.binding_version,
            "packed binding version",
        )?)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        let changed = sea_orm::sqlx::query("UPDATE ws_v1_workspaces SET head_epoch = ?, updated_at_ns = ? WHERE workspace_id = ? AND head_layer_id = ? AND head_epoch = ?")
            .bind(to_i64(record.head_epoch, "packed binding head epoch")?).bind(now_ns()?)
            .bind(record.workspace_id.as_bytes().as_slice()).bind(record.head_layer_id.as_bytes().as_slice()).bind(to_i64(request.guard.expected_head_epoch, "old head epoch")?)
            .execute(&mut *tx).await.map_err(backend)?;
        if changed.rows_affected() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        Self::allocate_sequences(&mut tx, record.head_layer_id, 1).await?;
        let reserved = sea_orm::sqlx::query(
            "UPDATE ws_v1_allocators SET next_value = ? WHERE name = 'inode' AND next_value = 2",
        )
        .bind(
            record
                .highest_inode
                .checked_add(1)
                .ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("packed inode floor overflow".into())
                })?
                .max(2),
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        if reserved.rows_affected() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let new_guard = HeadGuard {
            expected_head_epoch: record.head_epoch,
            ..request.guard
        };
        // Recheck expiry after staging every change. Failure rolls back binding,
        // epoch, sequence and allocator together; no lease is reacquired.
        Self::checked_guard(&mut tx, &new_guard).await?;
        #[cfg(test)]
        if self.failpoint.load(Ordering::SeqCst) == StoreFailpoint::BeforeCommit as u8 {
            return Err(WorkspaceError::Backend(
                "injected failure before packed binding commit".into(),
            ));
        }
        tx.commit().await.map_err(backend)?;
        Ok(record)
    }

    async fn publish_packed_lower_binding(
        &self,
        request: PublishPackedLowerBinding,
    ) -> Result<PackedLowerBindingRecord, WorkspaceError> {
        let record = request.record()?;
        let bytes = record.encode()?;
        let _writer = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let current = packed_binding_current_tx(&mut tx, request.guard.workspace_id).await?;
        if current.as_ref() == Some(&record) {
            let target_guard = HeadGuard {
                expected_head_epoch: record.head_epoch,
                ..request.guard.clone()
            };
            Self::checked_guard(&mut tx, &target_guard).await?;
            let previous_bytes: Option<Vec<u8>> = sea_orm::sqlx::query_scalar(
                "SELECT record FROM ws_v3_packed_bindings
                 WHERE workspace_id = ? AND binding_version = ?",
            )
            .bind(request.guard.workspace_id.as_bytes().as_slice())
            .bind(to_i64(
                request.expected_binding.binding.binding_version,
                "previous packed binding version",
            )?)
            .fetch_optional(&mut *tx)
            .await
            .map_err(backend)?;
            let previous =
                PackedLowerBindingRecord::decode(previous_bytes.as_deref().ok_or_else(|| {
                    WorkspaceError::CorruptMetadata("PWB3 predecessor history is missing".into())
                })?)?;
            if previous != request.expected_binding {
                return Err(WorkspaceError::Busy);
            }
            let committed_layers = [
                permission_layer_tx(&mut tx, request.expected_layers[0].layer_id).await?,
                permission_layer_tx(&mut tx, request.expected_layers[1].layer_id).await?,
            ];
            let base_revision = load_revision_tx(&mut tx, request.expected_base.layer_id).await?;
            let next_inode: Option<i64> = sea_orm::sqlx::query_scalar(
                "SELECT next_value FROM ws_v1_allocators WHERE name = 'inode'",
            )
            .fetch_optional(&mut *tx)
            .await
            .map_err(backend)?;
            let next_inode = next_inode.ok_or_else(|| {
                WorkspaceError::CorruptMetadata(
                    "inode allocator is missing during publication retry".into(),
                )
            })?;
            let target_guard = request.validate_committed_state(
                &record,
                &committed_layers,
                &base_revision,
                next_inode,
            )?;
            // A matching history row is insufficient after head rotation,
            // mutation, lease expiry or holder replacement. Validate the
            // publication's exact target epoch without weakening fencing.
            Self::checked_guard(&mut tx, &target_guard).await?;
            tx.commit().await.map_err(backend)?;
            return Ok(record);
        }
        Self::checked_guard(&mut tx, &request.guard).await?;
        for expected in &request.expected_layers {
            if permission_layer_tx(&mut tx, expected.layer_id).await? != *expected {
                return Err(WorkspaceError::Busy);
            }
        }
        if load_revision_tx(&mut tx, request.expected_base.layer_id).await? != request.expected_base
        {
            return Err(WorkspaceError::Busy);
        }
        if current.as_ref() != Some(&request.expected_binding) {
            return Err(WorkspaceError::Busy);
        }
        let old_version = to_i64(
            request.expected_binding.binding.binding_version,
            "packed binding version",
        )?;
        sea_orm::sqlx::query(
            "INSERT INTO ws_v3_packed_bindings(workspace_id, binding_version, record)
             VALUES (?, ?, ?)",
        )
        .bind(record.workspace_id.as_bytes().as_slice())
        .bind(to_i64(
            record.binding.binding_version,
            "packed binding version",
        )?)
        .bind(bytes)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        let current_changed = sea_orm::sqlx::query(
            "UPDATE ws_v3_packed_current
             SET binding_version = ?
             WHERE workspace_id = ? AND binding_version = ?",
        )
        .bind(to_i64(
            record.binding.binding_version,
            "packed binding version",
        )?)
        .bind(record.workspace_id.as_bytes().as_slice())
        .bind(old_version)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        if current_changed.rows_affected() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        let changed = sea_orm::sqlx::query(
            "UPDATE ws_v1_workspaces
             SET head_epoch = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND head_layer_id = ? AND head_epoch = ?",
        )
        .bind(to_i64(record.head_epoch, "packed binding head epoch")?)
        .bind(now_ns()?)
        .bind(record.workspace_id.as_bytes().as_slice())
        .bind(record.head_layer_id.as_bytes().as_slice())
        .bind(to_i64(request.guard.expected_head_epoch, "old head epoch")?)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        if changed.rows_affected() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        Self::allocate_sequences(&mut tx, record.head_layer_id, 1).await?;

        let floor = record
            .highest_inode
            .checked_add(1)
            .ok_or_else(|| WorkspaceError::CorruptMetadata("packed inode floor overflow".into()))?
            .max(2);
        let allocator: Option<i64> = sea_orm::sqlx::query_scalar(
            "SELECT next_value FROM ws_v1_allocators WHERE name = 'inode'",
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?;
        let allocator = allocator.ok_or_else(|| {
            WorkspaceError::CorruptMetadata("inode allocator is missing during publication".into())
        })?;
        request.validate_first_publication_allocator(allocator)?;
        if allocator < floor {
            let reserved = sea_orm::sqlx::query(
                "UPDATE ws_v1_allocators SET next_value = ?
                 WHERE name = 'inode' AND next_value = ?",
            )
            .bind(floor)
            .bind(allocator)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
            if reserved.rows_affected() != 1 {
                return Err(WorkspaceError::Fenced);
            }
        }
        let new_guard = HeadGuard {
            expected_head_epoch: record.head_epoch,
            ..request.guard
        };
        Self::checked_guard(&mut tx, &new_guard).await?;
        #[cfg(test)]
        if self.failpoint.load(Ordering::SeqCst) == StoreFailpoint::BeforeCommit as u8 {
            return Err(WorkspaceError::Backend(
                "injected failure before packed binding publication commit".into(),
            ));
        }
        tx.commit().await.map_err(backend)?;
        #[cfg(test)]
        if self.failpoint.load(Ordering::SeqCst) == StoreFailpoint::AfterPublicationCommit as u8 {
            return Err(WorkspaceError::Backend(
                "injected response loss after packed binding publication commit".into(),
            ));
        }
        Ok(record)
    }

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
        Self::migrate_workspace_columns(&self.pool).await?;
        Ok(())
    }

    async fn load_volume_header(&self) -> Result<Option<VolumeHeader>, WorkspaceError> {
        // init-volume probes the header before migration so an existing
        // catalog is never implicitly upgraded. A new SQL catalog has no
        // tables yet and must report the same absence as an empty KV store.
        let exists: i64 = sea_orm::sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name = 'ws_v1_volume_header'",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;
        if exists == 0 {
            return Ok(None);
        }
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
            "SELECT *
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

    async fn allocate_id_for_workspace(
        &self,
        workspace_id: WorkspaceId,
        name: &str,
    ) -> Result<i64, WorkspaceError> {
        if name != "slice" {
            return self.allocate_id(name).await;
        }
        let workspace = self.load_workspace(workspace_id).await?;
        let Some(bucket) = workspace.upper_bucket else {
            return self.allocate_id(name).await;
        };
        let allocator = format!("slice/{bucket}");
        let _guard = self.write_gate.lock().await;
        sea_orm::sqlx::query(
            "INSERT OR IGNORE INTO ws_v1_allocators(name, next_value) VALUES (?, 1)",
        )
        .bind(&allocator)
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        sea_orm::sqlx::query_scalar::<_, i64>(
            "UPDATE ws_v1_allocators SET next_value = next_value + 1
             WHERE name = ? AND next_value < ? RETURNING next_value - 1",
        )
        .bind(&allocator)
        .bind(i64::MAX)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| {
            WorkspaceError::CorruptMetadata("workspace slice allocator is exhausted".into())
        })
    }

    async fn create_volume_root(
        &self,
        request: CreateVolumeRoot,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        self.create_volume_root_inner(request, false).await
    }

    async fn create_workspace(
        &self,
        request: CreateWorkspace,
    ) -> Result<WorkspaceRecord, WorkspaceError> {
        Self::migrate_workspace_columns(&self.pool).await?;
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
             (workspace_id, upper_bucket, head_layer_id, head_epoch, fork_base_layer_id,
              fork_base_version, fork_base_root_hash, owner_id, state,
              created_at_ns, updated_at_ns)
             VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(request.upper_bucket.as_deref())
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
            upper_bucket: request.upper_bucket,
            head_layer_id: request.head_layer_id,
            head_epoch: 0,
            fork_base: Some(request.base_revision),
            owner_id: request.owner_id,
            state: WorkspaceState::Active,
            active_lease: None,
            created_at_ns: now,
            updated_at_ns: now,
        })
    }

    async fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, WorkspaceError> {
        let rows = sea_orm::sqlx::query(
            "SELECT *
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
        .ok_or(WorkspaceError::SnapshotNotFound(id))?;
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
            return Err(WorkspaceError::SnapshotNotFound(id));
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
            "SELECT w.state AS workspace_state, w.active_lease,
                    l.parent_layer_id, p.sealed_version, p.root_hash
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
        if let Some(existing) = row
            .try_get::<Option<Vec<u8>>, _>("active_lease")
            .map_err(backend)?
            .map(|bytes| uuid_from_blob(bytes, "workspace active lease").map(LeaseId::from_uuid))
            .transpose()?
        {
            let still_active = sea_orm::sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM ws_v1_snapshot_leases
                 WHERE lease_id = ? AND workspace_id = ? AND writable = 1
                   AND state = ? AND expires_at_ns > ?",
            )
            .bind(existing.as_bytes().as_slice())
            .bind(request.workspace_id.as_bytes().as_slice())
            .bind(LeaseState::Active.discriminant() as i64)
            .bind(now)
            .fetch_one(&mut *tx)
            .await
            .map_err(backend)?;
            if still_active != 0 {
                return Err(WorkspaceError::Busy);
            }
            // 清除已经失效的租约指针，允许重新获取租约。
            sea_orm::sqlx::query(
                "UPDATE ws_v1_snapshot_leases SET state = ?, updated_at_ns = ?
                 WHERE lease_id = ? AND workspace_id = ? AND state = ? AND expires_at_ns <= ?",
            )
            .bind(LeaseState::Expired.discriminant() as i64)
            .bind(now)
            .bind(existing.as_bytes().as_slice())
            .bind(request.workspace_id.as_bytes().as_slice())
            .bind(LeaseState::Active.discriminant() as i64)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
            sea_orm::sqlx::query(
                "UPDATE ws_v1_workspaces SET active_lease = NULL, updated_at_ns = ?
                 WHERE workspace_id = ? AND active_lease = ?",
            )
            .bind(now)
            .bind(request.workspace_id.as_bytes().as_slice())
            .bind(existing.as_bytes().as_slice())
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        }
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
        let pointed = sea_orm::sqlx::query(
            "UPDATE ws_v1_workspaces SET active_lease = ?, updated_at_ns = ?
             WHERE workspace_id = ? AND state = ? AND active_lease IS NULL",
        )
        .bind(request.lease_id.as_bytes().as_slice())
        .bind(now)
        .bind(request.workspace_id.as_bytes().as_slice())
        .bind(WorkspaceState::Active.discriminant() as i64)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        if pointed.rows_affected() != 1 {
            return Err(WorkspaceError::Busy);
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
        let mut tx = self.begin_write().await?;
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
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        if updated.rows_affected() != 1 {
            return Err(WorkspaceError::Fenced);
        }
        sea_orm::sqlx::query(
            "UPDATE ws_v1_workspaces SET active_lease = NULL, updated_at_ns = ?
             WHERE active_lease = ?",
        )
        .bind(now)
        .bind(request.lease_id.as_bytes().as_slice())
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn reap_expired_leases(&self) -> Result<u64, WorkspaceError> {
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        let now = now_ns()?;
        sea_orm::sqlx::query(
            "UPDATE ws_v1_workspaces SET active_lease = NULL, updated_at_ns = ?
             WHERE active_lease IN (
                 SELECT lease_id FROM ws_v1_snapshot_leases
                 WHERE state = ? AND expires_at_ns <= ?
             )",
        )
        .bind(now)
        .bind(LeaseState::Active.discriminant() as i64)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        let updated = sea_orm::sqlx::query(
            "UPDATE ws_v1_snapshot_leases SET state = ?, updated_at_ns = ?
             WHERE state = ? AND expires_at_ns <= ?",
        )
        .bind(LeaseState::Expired.discriminant() as i64)
        .bind(now)
        .bind(LeaseState::Active.discriminant() as i64)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(updated.rows_affected())
    }

    async fn prune_terminal_records(
        &self,
        now_ns: i64,
        grace_ns: u64,
    ) -> Result<(), WorkspaceError> {
        let cutoff = now_ns.saturating_sub(to_i64(grace_ns, "terminal record grace")?);
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        sea_orm::sqlx::query(
            "DELETE FROM ws_v1_snapshot_leases
             WHERE state IN (?, ?) AND updated_at_ns <= ?",
        )
        .bind(LeaseState::Released.discriminant() as i64)
        .bind(LeaseState::Expired.discriminant() as i64)
        .bind(cutoff)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sea_orm::sqlx::query(
            "DELETE FROM ws_v1_seal_journal
             WHERE phase IN (?, ?) AND updated_at_ns <= ?
               AND journal_id NOT IN (
                   SELECT journal_id FROM (
                       SELECT journal_id,
                              ROW_NUMBER() OVER (
                                  PARTITION BY workspace_id
                                  ORDER BY updated_at_ns DESC, journal_id DESC
                              ) AS rank
                       FROM ws_v1_seal_journal
                       WHERE phase IN (?, ?)
                   ) WHERE rank = 1
               )",
        )
        .bind(SealPhase::Completed.discriminant() as i64)
        .bind(SealPhase::Aborted.discriminant() as i64)
        .bind(cutoff)
        .bind(SealPhase::Completed.discriminant() as i64)
        .bind(SealPhase::Aborted.discriminant() as i64)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sea_orm::sqlx::query(
            "UPDATE ws_v1_workspaces SET active_lease = NULL
             WHERE active_lease IS NOT NULL
               AND active_lease NOT IN (
                   SELECT lease_id FROM ws_v1_snapshot_leases
                   WHERE state = ? AND expires_at_ns > ?
               )",
        )
        .bind(LeaseState::Active.discriminant() as i64)
        .bind(now_ns)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    #[cfg(test)]
    async fn install_test_base(
        &self,
        request: InstallTestBase,
    ) -> Result<TestBaseResult, WorkspaceError> {
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
            "SELECT parent_layer_id FROM ws_v1_layers
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
        if parent != request.expected_parent_layer_id {
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
        Ok(TestBaseResult {
            revision: BaseRevision {
                layer_id: request.compacted_layer_id,
                sealed_version,
                root_hash: root,
            },
            replacement_head_layer_id: request.replacement_head_layer_id,
            head_epoch: epoch,
        })
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

    fn supports_versioned_permissions(&self) -> bool {
        true
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
        // One SQLite snapshot spans every row, including absent ACL/dentry
        // rows. Independent readers cannot combine mode and ACL commits.
        let mut tx = self.pool.begin().await.map_err(backend)?;
        let head = permission_layer_tx(&mut tx, request.layer_ids[0]).await?;
        let base = permission_layer_tx(&mut tx, request.layer_ids[1]).await?;
        let mut snapshot = PermissionSnapshot {
            layers: [head, base],
            inodes: Vec::new(),
            xattrs: Vec::new(),
            dentries: Vec::new(),
        };
        validate_layer_chain(request.layer_ids[0], &snapshot.layers)?;
        if snapshot.layers[0].state != LayerState::Writable
            || snapshot.layers[0].depth != 2
            || snapshot.layers[0].parent_layer_id != Some(request.layer_ids[1])
            || snapshot.layers[1].state != LayerState::Sealed
            || snapshot.layers[1].depth != 1
            || snapshot.layers[1].parent_layer_id.is_some()
        {
            return Err(WorkspaceError::Fenced);
        }
        for ino in request.inodes {
            for layer in request.layer_ids {
                if let Some(row) = sea_orm::sqlx::query(
                    "SELECT layer_id, ino, state, kind, size, mode, uid, gid, rdev, nlink,
                            atime_ns, mtime_ns, ctime_ns, symlink_target, parent_hint, data_version, sequence
                     FROM ws_v1_inode_delta WHERE layer_id = ? AND ino = ?")
                    .bind(layer.as_bytes().as_slice()).bind(ino).fetch_optional(&mut *tx).await.map_err(backend)? {
                    snapshot.inodes.push(decode_inode(&row)?);
                }
                for name in [ACCESS_XATTR, DEFAULT_XATTR, b"system.brewfs.acl".as_slice()] {
                    if let Some(row) = sea_orm::sqlx::query(
                        "SELECT layer_id, ino, name, op, value, sequence FROM ws_v1_xattr_delta
                         WHERE layer_id = ? AND ino = ? AND name = ?",
                    )
                    .bind(layer.as_bytes().as_slice())
                    .bind(ino)
                    .bind(name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(backend)?
                    {
                        snapshot.xattrs.push(decode_xattr(&row)?);
                    }
                }
            }
        }
        if let Some((parent, name)) = request.dentry {
            for layer in request.layer_ids {
                if let Some(row) = sea_orm::sqlx::query(
                    "SELECT layer_id, parent_ino, name, op, ino, entry_type, sequence
                     FROM ws_v1_dentry_delta WHERE layer_id = ? AND parent_ino = ? AND name = ?",
                )
                .bind(layer.as_bytes().as_slice())
                .bind(parent)
                .bind(&name)
                .fetch_optional(&mut *tx)
                .await
                .map_err(backend)?
                {
                    snapshot.dentries.push(decode_dentry(row)?);
                }
            }
        }
        tx.commit().await.map_err(backend)?;
        Ok(snapshot)
    }

    async fn apply_permission_mutation(
        &self,
        request: PermissionMutation,
    ) -> Result<MutationResult, WorkspaceError> {
        self.apply_versioned_mutation(VersionedMutation {
            guard: request.guard,
            expected_layers: request.expected_layers,
            dentries: request.dentries,
            inodes: request.inodes,
            xattrs: request.xattrs,
            acls: Vec::new(),
            extents: Vec::new(),
            chunk_size: 1,
        })
        .await
    }

    async fn apply_versioned_mutation(
        &self,
        request: VersionedMutation,
    ) -> Result<MutationResult, WorkspaceError> {
        let count = request.validate()?;
        let _local_gate = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        Self::checked_guard(&mut tx, &request.guard).await?;
        let head = request.guard.expected_head_layer_id;
        for expected in &request.expected_layers {
            if permission_layer_tx(&mut tx, expected.layer_id).await? != *expected {
                return Err(WorkspaceError::Busy);
            }
        }
        let Some((first, last)) = Self::allocate_sequences(&mut tx, head, count).await? else {
            tx.commit().await.map_err(backend)?;
            return Ok(MutationResult {
                first_sequence: None,
                last_sequence: None,
            });
        };
        let mut sequence = first;
        for mut row in request.dentries {
            row.sequence = sequence;
            sequence += 1;
            upsert_dentry(&mut tx, &row).await?;
        }
        for mut row in request.xattrs {
            row.sequence = sequence;
            sequence += 1;
            upsert_xattr(&mut tx, &row).await?;
        }
        for mut row in request.acls {
            row.sequence = sequence;
            sequence += 1;
            upsert_acl(&mut tx, &row).await?;
        }
        for mut row in request.inodes {
            row.sequence = sequence;
            sequence += 1;
            upsert_inode(&mut tx, &row).await?;
        }
        let mut owned_count = 0u64;
        let mut owned_bytes = 0u64;
        for mut row in request.extents {
            row.sequence = sequence;
            sequence += 1;
            if matches!(row.kind, ExtentKind::Data { .. }) {
                owned_count = owned_count
                    .checked_add(1)
                    .ok_or_else(|| WorkspaceError::Backend("owned count overflows".into()))?;
                owned_bytes = owned_bytes
                    .checked_add(row.length)
                    .ok_or_else(|| WorkspaceError::Backend("owned bytes overflow".into()))?;
            }
            insert_extent(&mut tx, &row).await?;
        }
        if owned_count > 0 {
            sea_orm::sqlx::query("UPDATE ws_v1_layers SET owned_slice_count = owned_slice_count + ?, owned_bytes = owned_bytes + ? WHERE layer_id = ?")
                .bind(to_i64(owned_count, "owned count")?).bind(to_i64(owned_bytes, "owned bytes")?)
                .bind(head.as_bytes().as_slice()).execute(&mut *tx).await.map_err(backend)?;
        }
        #[cfg(test)]
        if self.failpoint.load(Ordering::SeqCst) == StoreFailpoint::BeforeCommit as u8 {
            return Err(WorkspaceError::Backend(
                "injected failure before transaction commit".into(),
            ));
        }
        tx.commit().await.map_err(backend)?;
        Ok(MutationResult {
            first_sequence: Some(first),
            last_sequence: Some(last),
        })
    }

    async fn validate_read_fence(
        &self,
        guard: HeadGuard,
        expected_layers: [LayerRecord; 2],
    ) -> Result<(), WorkspaceError> {
        let mut tx = self.pool.begin().await.map_err(backend)?;
        Self::checked_guard(&mut tx, &guard).await?;
        for expected in &expected_layers {
            if permission_layer_tx(&mut tx, expected.layer_id).await? != *expected {
                return Err(WorkspaceError::Busy);
            }
        }
        tx.commit().await.map_err(backend)
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
        let chunk_index = to_i64(request.chunk_index, "chunk index")?;
        let range_start = to_i64(request.range_start, "range start")?;
        let range_end = to_i64(request.range_end, "range end")?;
        let mut rows = Vec::with_capacity(max_rows);
        for layer_id in request.layer_ids {
            // Fetch at most remaining+1 fixed-width rows to detect overflow.
            // The sentinel is charged by the caller before this query.
            let limit = i64::try_from(max_rows - rows.len() + 1)
                .map_err(|_| WorkspaceError::InvalidReadPlan("extent limit overflows".into()))?;
            let found = sea_orm::sqlx::query(
                "SELECT layer_id, ino, chunk_index, logical_offset, length, kind,
                        slice_id, slice_offset, sequence
                 FROM ws_v1_data_extent_delta
                 WHERE layer_id = ? AND ino = ? AND chunk_index = ?
                   AND logical_offset < ? AND logical_offset + length > ?
                 ORDER BY sequence DESC LIMIT ?",
            )
            .bind(layer_id.as_bytes().as_slice())
            .bind(request.ino)
            .bind(chunk_index)
            .bind(range_end)
            .bind(range_start)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
            if found.len() > max_rows - rows.len() {
                return Err(WorkspaceError::InvalidReadPlan(
                    "upper extent row limit exceeded".into(),
                ));
            }
            for row in &found {
                rows.push(decode_extent(row)?);
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

    async fn load_seal_journal(
        &self,
        journal_id: JournalId,
    ) -> Result<SealJournal, WorkspaceError> {
        load_seal_journal_pool(&self.pool, journal_id).await
    }

    async fn list_seal_journals(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<SealJournal>, WorkspaceError> {
        let rows = sea_orm::sqlx::query(
            "SELECT journal_id, workspace_id, old_head_layer_id, expected_head_epoch,
                    phase, pending_bytes, delta_digest, root_hash, new_head_layer_id,
                    last_error, created_at_ns, updated_at_ns
             FROM ws_v1_seal_journal WHERE workspace_id = ?
             ORDER BY created_at_ns, journal_id",
        )
        .bind(workspace_id.as_bytes().as_slice())
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        rows.iter().map(decode_seal_journal).collect()
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
            "UPDATE ws_v1_workspaces SET state = ?, active_lease = NULL, updated_at_ns = ?
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
        for workspace in self.list_workspaces().await? {
            if workspace.upper_bucket.is_some() {
                return Err(WorkspaceError::UnsupportedCapability(
                    "legacy GC cannot collect bucket-isolated workspaces; use explicit upper bucket cleanup",
                ));
            }
            if workspace.state != WorkspaceState::Deleting {
                roots.insert(workspace.head_layer_id);
            }
        }
        let lease_roots = sea_orm::sqlx::query(
            "SELECT base_layer_id FROM ws_v1_snapshot_leases
             WHERE state IN (?, ?, ?) AND expires_at_ns > ?",
        )
        .bind(LeaseState::Active.discriminant() as i64)
        .bind(LeaseState::Releasing.discriminant() as i64)
        .bind(LeaseState::Expired.discriminant() as i64)
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
        // PWB3 binding history is an independent catalog root. A workspace
        // can enter Deleting before its binding records are retired; dropping
        // the native base/head here would leave a durable binding dangling.
        let packed_bindings = sea_orm::sqlx::query("SELECT record FROM ws_v3_packed_bindings")
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
        for row in packed_bindings {
            let record = PackedLowerBindingRecord::decode(
                &row.try_get::<Vec<u8>, _>("record").map_err(backend)?,
            )?;
            roots.insert(record.base_revision.layer_id);
            roots.insert(record.head_layer_id);
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
        // PWB3 binding history is an independent catalog root. The regular
        // workspace/snapshot/lease/journal roots below are not sufficient
        // after a workspace enters Deleting: a binding can still reference
        // its sealed base and writable head. Read and authenticate every
        // persisted history record inside this same transaction so the
        // destructive recheck cannot race a stale gc_snapshot result.
        let packed_binding_rows = sea_orm::sqlx::query("SELECT record FROM ws_v3_packed_bindings")
            .fetch_all(&mut *tx)
            .await
            .map_err(backend)?;
        let mut packed_binding_roots = BTreeSet::new();
        for row in packed_binding_rows {
            let record = PackedLowerBindingRecord::decode(
                &row.try_get::<Vec<u8>, _>("record").map_err(backend)?,
            )?;
            packed_binding_roots.insert(record.base_revision.layer_id);
            packed_binding_roots.insert(record.head_layer_id);
        }
        let layer_rows = sea_orm::sqlx::query("SELECT layer_id, parent_layer_id FROM ws_v1_layers")
            .fetch_all(&mut *tx)
            .await
            .map_err(backend)?;
        let mut layer_parents = BTreeMap::new();
        for row in layer_rows {
            let layer_id = LayerId::from_uuid(uuid_from_blob(
                row.try_get::<Vec<u8>, _>("layer_id").map_err(backend)?,
                "packed binding layer root",
            )?);
            let parent = row
                .try_get::<Option<Vec<u8>>, _>("parent_layer_id")
                .map_err(backend)?
                .map(|bytes| uuid_from_blob(bytes, "packed binding parent"))
                .transpose()?
                .map(LayerId::from_uuid);
            layer_parents.insert(layer_id, parent);
        }
        let mut packed_binding_reachable = BTreeSet::new();
        let mut pending = packed_binding_roots.iter().copied().collect::<Vec<_>>();
        while let Some(layer_id) = pending.pop() {
            if !packed_binding_reachable.insert(layer_id) {
                continue;
            }
            if let Some(Some(parent)) = layer_parents.get(&layer_id) {
                pending.push(*parent);
            }
        }
        for layer_id in &request.layer_ids {
            if packed_binding_reachable.contains(layer_id) {
                return Err(WorkspaceError::Busy);
            }
            let reachable = sea_orm::sqlx::query_scalar::<_, i64>(
                "WITH RECURSIVE roots(layer_id) AS (
                     SELECT head_layer_id FROM ws_v1_workspaces WHERE state != ?
                     UNION SELECT layer_id FROM ws_v1_snapshots
                     UNION SELECT base_layer_id FROM ws_v1_snapshot_leases
                       WHERE state IN (?, ?, ?) AND expires_at_ns > ?
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
            .bind(LeaseState::Expired.discriminant() as i64)
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

    async fn reserve_gc_slice_deletion(
        &self,
        _slice_id: u64,
        _retained_slice_end: u64,
        _deleted_layers: &[LayerId],
    ) -> Result<(), WorkspaceError> {
        // SQLite is not a production packed-v3 deletion-fence implementation.
        // Explicit compatibility is limited to existing legacy unit fixtures.
        #[cfg(test)]
        {
            let header = self
                .load_volume_header()
                .await?
                .ok_or(WorkspaceError::Fenced)?;
            let packed: i64 =
                sea_orm::sqlx::query_scalar("SELECT COUNT(*) FROM ws_v3_packed_bindings")
                    .fetch_one(&self.pool)
                    .await
                    .map_err(backend)?;
            if header.volume_format == VOLUME_FORMAT && packed == 0 {
                return Ok(());
            }
        }
        Err(WorkspaceError::UnsupportedCapability(
            "SQLite durable SID deletion fence",
        ))
    }

    async fn finalize_layer_metadata_deletion(
        &self,
        layer_ids: Vec<LayerId>,
    ) -> Result<(), WorkspaceError> {
        if layer_ids.is_empty() {
            return Ok(());
        }
        let _guard = self.write_gate.lock().await;
        let mut tx = self.begin_write().await?;
        // Deleting is a lifecycle state, not proof that the layer is orphaned:
        // deleting a workspace can leave its head in this state while durable
        // PWB3 history still owns it. Repeat the binding-root/ancestry check
        // under the same write transaction as the irreversible finalization.
        let packed_reachable = packed_binding_reachable_tx(&mut tx).await?;
        if layer_ids
            .iter()
            .any(|layer| packed_reachable.contains(layer))
        {
            return Err(WorkspaceError::Busy);
        }
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
}

async fn packed_binding_reachable_tx(
    tx: &mut Transaction<'_, Sqlite>,
) -> Result<BTreeSet<LayerId>, WorkspaceError> {
    let records: Vec<Vec<u8>> =
        sea_orm::sqlx::query_scalar("SELECT record FROM ws_v3_packed_bindings")
            .fetch_all(&mut **tx)
            .await
            .map_err(backend)?;
    let mut pending = Vec::new();
    for bytes in records {
        let record = PackedLowerBindingRecord::decode(&bytes)?;
        pending.push(record.base_revision.layer_id);
        pending.push(record.head_layer_id);
    }
    if pending.is_empty() {
        return Ok(BTreeSet::new());
    }
    let rows = sea_orm::sqlx::query("SELECT layer_id, parent_layer_id FROM ws_v1_layers")
        .fetch_all(&mut **tx)
        .await
        .map_err(backend)?;
    let mut parents = BTreeMap::new();
    for row in rows {
        let layer = LayerId::from_uuid(uuid_from_blob(
            row.try_get::<Vec<u8>, _>("layer_id").map_err(backend)?,
            "packed binding layer root",
        )?);
        let parent = row
            .try_get::<Option<Vec<u8>>, _>("parent_layer_id")
            .map_err(backend)?
            .map(|bytes| uuid_from_blob(bytes, "packed binding parent"))
            .transpose()?
            .map(LayerId::from_uuid);
        parents.insert(layer, parent);
    }
    let mut reachable = BTreeSet::new();
    while let Some(layer) = pending.pop() {
        if !reachable.insert(layer) {
            continue;
        }
        if let Some(Some(parent)) = parents.get(&layer) {
            pending.push(*parent);
        }
    }
    Ok(reachable)
}

async fn packed_binding_current_tx(
    tx: &mut Transaction<'_, Sqlite>,
    workspace_id: WorkspaceId,
) -> Result<Option<PackedLowerBindingRecord>, WorkspaceError> {
    let version: Option<i64> = sea_orm::sqlx::query_scalar(
        "SELECT binding_version FROM ws_v3_packed_current WHERE workspace_id = ?",
    )
    .bind(workspace_id.as_bytes().as_slice())
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?;
    let Some(version) = version else {
        let claimed: i64 = sea_orm::sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM ws_v3_packed_bindings WHERE workspace_id = ?)",
        )
        .bind(workspace_id.as_bytes().as_slice())
        .fetch_one(&mut **tx)
        .await
        .map_err(backend)?;
        if claimed != 0 {
            return Err(WorkspaceError::CorruptMetadata(
                "PWB3 claim has no current pointer".into(),
            ));
        }
        return Ok(None);
    };
    let bytes: Vec<u8> = sea_orm::sqlx::query_scalar(
        "SELECT record FROM ws_v3_packed_bindings WHERE workspace_id = ? AND binding_version = ?",
    )
    .bind(workspace_id.as_bytes().as_slice())
    .bind(version)
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?
    .ok_or_else(|| WorkspaceError::CorruptMetadata("PWB3 current history missing".into()))?;
    let record = PackedLowerBindingRecord::decode(&bytes)?;
    if record.workspace_id != workspace_id
        || record.binding.binding_version != to_u64(version, "packed binding version")?
    {
        return Err(WorkspaceError::CorruptMetadata(
            "PWB3 current key/record disagree".into(),
        ));
    }
    // The version-1 history row is the durable lineage anchor. Every reader,
    // including v3 open/remount recovery, must reject a current pointer that
    // outlived that anchor instead of treating the torn state as Ready.
    let initial_bytes: Option<Vec<u8>> = sea_orm::sqlx::query_scalar(
        "SELECT record FROM ws_v3_packed_bindings
         WHERE workspace_id = ? AND binding_version = 1",
    )
    .bind(workspace_id.as_bytes().as_slice())
    .fetch_optional(&mut **tx)
    .await
    .map_err(backend)?;
    let initial = PackedLowerBindingRecord::decode(initial_bytes.as_deref().ok_or_else(|| {
        WorkspaceError::CorruptMetadata("PWB3 initial history is missing".into())
    })?)?;
    if initial.workspace_id != workspace_id || initial.binding.binding_version != 1 {
        return Err(WorkspaceError::CorruptMetadata(
            "PWB3 initial history key/record disagree".into(),
        ));
    }
    Ok(Some(record))
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
        "SELECT *
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
        upper_bucket: match row.try_get("upper_bucket") {
            Ok(value) => value,
            Err(sea_orm::sqlx::Error::ColumnNotFound(_)) => None,
            Err(error) => return Err(backend(error)),
        },
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
        active_lease: row
            .try_get::<Option<Vec<u8>>, _>("active_lease")
            .map_err(backend)?
            .map(|bytes| uuid_from_blob(bytes, "workspace active lease").map(LeaseId::from_uuid))
            .transpose()?,
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

async fn permission_layer_tx(
    tx: &mut Transaction<'_, Sqlite>,
    layer: LayerId,
) -> Result<LayerRecord, WorkspaceError> {
    let row = sea_orm::sqlx::query("SELECT layer_id, parent_layer_id, state, schema_version, sealed_version, delta_digest, root_hash,
        depth, owner_workspace_id, next_sequence, owned_slice_count, owned_bytes, created_at_ns, sealed_at_ns
        FROM ws_v1_layers WHERE layer_id = ?")
        .bind(layer.as_bytes().as_slice()).fetch_optional(&mut **tx).await.map_err(backend)?
        .ok_or(WorkspaceError::LayerNotFound(layer))?;
    decode_layer(&row)
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

fn duration_ns(duration: Duration) -> Result<i64, WorkspaceError> {
    i64::try_from(duration.as_nanos())
        .map_err(|_| WorkspaceError::Backend("duration exceeds SQLite range".into()))
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

#[cfg(test)]
mod manager_init_tests {
    use super::*;

    fn request() -> CreateVolumeRoot {
        CreateVolumeRoot {
            upper_bucket: None,
            volume_format: VOLUME_FORMAT.into(),
            schema_version: WORKSPACE_SCHEMA_VERSION,
            volume_id: Uuid::new_v4(),
            workspace_id: WorkspaceId::new(),
            root_layer_id: LayerId::new(),
            writable_layer_id: LayerId::new(),
            owner_id: Some("manager-test".into()),
        }
    }

    #[tokio::test]
    async fn existing_catalog_connection_never_creates_an_absent_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.sqlite");
        let url = format!("sqlite://{}", path.display());
        assert!(SqliteWorkspaceStore::connect_existing(&url).await.is_err());
        assert!(!path.exists());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);

        let created = SqliteWorkspaceStore::connect(&url).await.unwrap();
        let workspace = created
            .create_volume_root_in_empty_namespace(request())
            .await
            .unwrap();
        created.pool.close().await;
        let reopened = SqliteWorkspaceStore::connect_existing(&url).await.unwrap();
        assert_eq!(reopened.list_workspaces().await.unwrap(), vec![workspace]);
        reopened.pool.close().await;

        // A previous existence check cannot authorize recreating a catalog
        // that disappeared before SQLite opened it.
        std::fs::remove_file(&path).unwrap();
        assert!(SqliteWorkspaceStore::connect_existing(&url).await.is_err());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn manager_packed_probe_detects_orphan_rows_without_initial_history() {
        let store = SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.initialize_workspace_schema().await.unwrap();
        assert!(!store.has_packed_catalog_bindings().await.unwrap());
        // An imported version-2 row need not have a workspace or version 1.
        // Its bytes are intentionally undecodable: any row must close native admission.
        sea_orm::sqlx::query("INSERT INTO ws_v3_packed_bindings VALUES (?, 2, X'01')")
            .bind(Uuid::new_v4().as_bytes().as_slice())
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(store.has_packed_catalog_bindings().await.unwrap());
        sea_orm::sqlx::query("DELETE FROM ws_v3_packed_bindings")
            .execute(&store.pool)
            .await
            .unwrap();
        // Model a partially restored/corrupt current pointer without history.
        // This isolated in-memory pool has one connection.
        sea_orm::sqlx::query("PRAGMA foreign_keys = OFF")
            .execute(&store.pool)
            .await
            .unwrap();
        sea_orm::sqlx::query("INSERT INTO ws_v3_packed_current VALUES (?, 2)")
            .bind(Uuid::new_v4().as_bytes().as_slice())
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(store.has_packed_catalog_bindings().await.unwrap());
        assert!(store.list_workspaces().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn manager_schema_probe_rejects_partial_header_and_workspace_catalog_without_writes() {
        let store = SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap();
        // An empty workspace list must not make an incomplete catalog look ready.
        for statement in SCHEMA.split(';').take(2) {
            sea_orm::sqlx::query(statement)
                .execute(&store.pool)
                .await
                .unwrap();
        }
        let before = store.schema_table_names().await.unwrap();
        assert_eq!(before.len(), 2);
        assert!(matches!(
            store.validate_manager_catalog_schema().await,
            Err(WorkspaceError::UnsupportedCapability(
                "complete workspace-v1 SQLite catalog schema"
            ))
        ));
        assert_eq!(store.schema_table_names().await.unwrap(), before);
    }

    #[tokio::test]
    async fn manager_schema_probe_accepts_complete_catalog_and_rejects_missing_column() {
        let store = SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap();
        let created = store
            .create_volume_root_in_empty_namespace(request())
            .await
            .unwrap();
        store.validate_manager_catalog_schema().await.unwrap();
        let header = store.load_volume_header().await.unwrap();
        sea_orm::sqlx::query(
            "ALTER TABLE ws_v1_allocators RENAME COLUMN next_value TO foreign_value",
        )
        .execute(&store.pool)
        .await
        .unwrap();
        assert!(matches!(
            store.validate_manager_catalog_schema().await,
            Err(WorkspaceError::UnsupportedCapability(
                "complete workspace-v1 SQLite catalog schema"
            ))
        ));
        assert_eq!(store.load_volume_header().await.unwrap(), header);
        assert_eq!(
            store.load_workspace(created.workspace_id).await.unwrap(),
            created
        );
    }

    #[tokio::test]
    async fn strict_initialize_rejects_foreign_schema_and_preserves_data() {
        let store = SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.initialize_workspace_schema().await.unwrap();
        // Deliberately starts with `sqlite` but not SQLite's reserved `sqlite_`.
        // A LIKE 'sqlite_%' probe would accidentally skip this foreign table.
        sea_orm::sqlx::raw_sql("CREATE TABLE sqliteXforeign(value TEXT); INSERT INTO sqliteXforeign VALUES ('keep-me');")
            .execute(&store.pool).await.unwrap();
        let result = store.create_volume_root_in_empty_namespace(request()).await;
        assert!(result.is_err(), "foreign namespace was initialized");
        assert!(store.load_volume_header().await.unwrap().is_none());
        let value: String = sea_orm::sqlx::query_scalar("SELECT value FROM sqliteXforeign")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(value, "keep-me");
    }

    #[tokio::test]
    async fn strict_initialize_rechecks_catalog_rows_not_just_volume_marker() {
        let store = SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap();
        store.initialize_workspace_schema().await.unwrap();
        sea_orm::sqlx::query("INSERT INTO ws_v1_allocators VALUES ('orphan', 7)")
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(
            store
                .create_volume_root_in_empty_namespace(request())
                .await
                .is_err()
        );
        assert!(store.load_volume_header().await.unwrap().is_none());
        let retained: i64 = sea_orm::sqlx::query_scalar(
            "SELECT next_value FROM ws_v1_allocators WHERE name = 'orphan'",
        )
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(retained, 7);
    }

    #[tokio::test]
    async fn strict_initialize_rejects_extra_triggers_and_views() {
        for ddl in [
            "CREATE TRIGGER foreign_trigger BEFORE INSERT ON ws_v1_workspaces BEGIN SELECT 1; END;",
            "CREATE VIEW foreign_view AS SELECT * FROM ws_v1_workspaces;",
        ] {
            let store = SqliteWorkspaceStore::connect("sqlite::memory:")
                .await
                .unwrap();
            store.initialize_workspace_schema().await.unwrap();
            sea_orm::sqlx::raw_sql(ddl)
                .execute(&store.pool)
                .await
                .unwrap();
            assert!(
                store
                    .create_volume_root_in_empty_namespace(request())
                    .await
                    .is_err()
            );
            assert!(store.load_volume_header().await.unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn strict_initialize_recovers_empty_schema_but_never_replaces_a_volume() {
        let store = SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap();
        // A crash after a prefix of the CREATE TABLE statements can leave an
        // empty partial schema. Completing that schema does not claim data.
        sea_orm::sqlx::raw_sql(SCHEMA.split(';').next().unwrap())
            .execute(&store.pool)
            .await
            .unwrap();
        let identity = request();
        let created = store
            .create_volume_root_in_empty_namespace(identity.clone())
            .await
            .unwrap();
        assert_eq!(created.workspace_id, identity.workspace_id);
        assert!(
            store
                .create_volume_root_in_empty_namespace(request())
                .await
                .is_err()
        );
        assert_eq!(
            store.load_volume_header().await.unwrap().unwrap().volume_id,
            identity.volume_id
        );
        assert_eq!(store.list_workspaces().await.unwrap(), vec![created]);
    }

    #[tokio::test]
    async fn strict_initialize_refuses_foreign_database_without_adding_schema() {
        let store = SqliteWorkspaceStore::connect("sqlite::memory:")
            .await
            .unwrap();
        sea_orm::sqlx::raw_sql("CREATE TABLE application_data(value TEXT); INSERT INTO application_data VALUES ('keep-me');")
            .execute(&store.pool).await.unwrap();
        assert!(
            store
                .create_volume_root_in_empty_namespace(request())
                .await
                .is_err()
        );
        assert!(store.schema_table_names().await.unwrap().is_empty());
        let value: String = sea_orm::sqlx::query_scalar("SELECT value FROM application_data")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(value, "keep-me");
    }
}
