//! Single-instance, durable lifecycle coordinator. Catalogs remain authoritative.

use crate::workspace_overlay::{
    admin,
    catalog::{CreateVolumeRoot, CreateWorkspace},
    error::WorkspaceError,
    ids::{LayerId, WorkspaceId},
    model::{
        BaseRevision, VolumeHeader, WORKSPACE_SCHEMA_VERSION, WorkspaceRecord, WorkspaceState,
    },
};
#[cfg(test)]
use crate::workspace_overlay::{catalog::WorkspaceStore, stores::database::SqliteWorkspaceStore};
use sea_orm::sqlx::{
    self, Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;
use uuid::Uuid;

mod catalog;
#[cfg(feature = "native-packed-base")]
mod native;
mod revision;
use catalog::{CatalogConfig, CatalogConnection};
pub use revision::{NativeCatalogRevision, NativeRevisionKind, Revision};

#[cfg(test)]
mod backend_tests;
#[cfg(test)]
mod binding_tests;
#[cfg(all(test, feature = "native-packed-base"))]
mod native_backend_tests;
#[cfg(test)]
mod tests;

pub const MAX_FORK_COUNT: usize = 100;
const MANAGER_SESSION_TTL_NS: u64 = 30_000_000_000;
const MAX_PENDING_OPERATIONS: i64 = 128;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct ManagerError {
    pub code: &'static str,
    pub message: String,
}
impl ManagerError {
    fn new(code: &'static str, message: &str) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}
type Result<T> = std::result::Result<T, ManagerError>;
fn storage(_: impl std::fmt::Display) -> ManagerError {
    ManagerError::new(
        "unavailable",
        "manager storage is unavailable; inspect local service diagnostics",
    )
}
fn invalid(message: &str) -> ManagerError {
    ManagerError::new("invalid_request", message)
}
fn conflict(message: &str) -> ManagerError {
    ManagerError::new("conflict", message)
}
fn catalog_error(error: WorkspaceError) -> ManagerError {
    match error {
        WorkspaceError::WorkspaceNotFound(_)
        | WorkspaceError::LayerNotFound(_)
        | WorkspaceError::SnapshotNotFound(_) => {
            ManagerError::new("not_found", "catalog resource not found")
        }
        WorkspaceError::Busy
        | WorkspaceError::Fenced
        | WorkspaceError::Conflict(_)
        | WorkspaceError::InvalidStateTransition { .. } => conflict(
            "catalog resource is leased, referenced, or conflicts with the recorded operation",
        ),
        WorkspaceError::UnsupportedVolumeFormat(_)
        | WorkspaceError::UnsupportedSchemaVersion(_)
        | WorkspaceError::UnsupportedCapability(_)
        | WorkspaceError::FeatureNotCompiled(_) => ManagerError::new(
            "unsupported",
            "catalog format or capability is unsupported by this Manager",
        ),
        WorkspaceError::CorruptMetadata(_)
        | WorkspaceError::InvalidReadPlan(_)
        | WorkspaceError::LayerDepthLimit { .. } => {
            conflict("catalog identity or revision does not match the recorded request")
        }
        _ => ManagerError::new(
            "recovery_required",
            "catalog outcome requires reconciliation; retry the original request after restoring backend access",
        ),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VolumeMode {
    Register,
    Initialize,
}
fn format_default() -> String {
    "workspace-v1".into()
}
fn count_default() -> usize {
    1
}
fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 128 || !key.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(invalid(
            "Idempotency-Key must contain 1..128 visible ASCII bytes",
        ));
    }
    Ok(())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeRequest {
    pub mode: VolumeMode,
    pub catalog_ref: String,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub expected_volume_id: Option<Uuid>,
    #[serde(default)]
    pub confirm_empty: bool,
    #[serde(default = "format_default")]
    pub volume_format: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkRequest {
    pub revision: Revision,
    #[serde(default = "count_default")]
    pub count: usize,
    #[serde(default)]
    pub owner: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VolumeView {
    pub id: Uuid,
    pub catalog_ref: String,
    pub volume_format: String,
    pub schema_version: u32,
    pub metadata_ready: bool,
    pub root_workspace_id: Option<WorkspaceId>,
    pub root_revision: Option<Revision>,
    /// No remote durability or mount evidence is produced by this coordinator.
    pub remote_ready: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_namespace_id: Option<[u8; 16]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_control_version: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkspaceView {
    pub record: WorkspaceRecord,
    pub base_revision: Revision,
    pub layer_depth: u32,
    pub leases: Vec<crate::workspace_overlay::model::SnapshotLease>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native: Option<serde_json::Value>,
    pub mount_identity: MountIdentity,
}

/// Non-secret identities to combine with a client's independently configured
/// connection settings. This is not a mount or remote-durability receipt.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountIdentity {
    pub catalog_ref: String,
    pub metadata_backend: String,
    pub volume_format: String,
    pub volume_id: Uuid,
    pub workspace_id: WorkspaceId,
    pub workspace_namespace: Option<String>,
    pub storage_namespace_id: Option<[u8; 16]>,
    pub upper_bucket: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NativeCreation {
    domain_id: Uuid,
    owner_id: Uuid,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Accepted,
    Running,
    Succeeded,
    Failed,
    RecoveryRequired,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ItemState {
    Pending,
    Succeeded,
    Failed,
    RecoveryRequired,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperationError {
    pub code: String,
    pub message: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperationItem {
    pub workspace_id: Option<WorkspaceId>,
    pub head_layer_id: Option<LayerId>,
    pub state: ItemState,
    pub metadata_ready: bool,
    pub remote_ready: Option<bool>,
    pub deletion_marked: bool,
    pub physical_reclaimed: bool,
    pub error: Option<OperationError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    native_creation: Option<NativeCreation>,
}
impl OperationItem {
    fn new(workspace_id: Option<WorkspaceId>, head_layer_id: Option<LayerId>) -> Self {
        Self {
            workspace_id,
            head_layer_id,
            state: ItemState::Pending,
            metadata_ready: false,
            remote_ready: None,
            deletion_marked: false,
            physical_reclaimed: false,
            error: None,
            native_creation: None,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperationEvent {
    pub at: i64,
    pub state: OperationState,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub id: Uuid,
    pub volume_id: Uuid,
    pub kind: String,
    pub state: OperationState,
    pub items: Vec<OperationItem>,
    pub error: Option<OperationError>,
    pub created_at: i64,
    pub updated_at: i64,
    pub events: Vec<OperationEvent>,
}
impl Operation {
    fn new(volume_id: Uuid, kind: &str, items: Vec<OperationItem>) -> Self {
        let at = chrono::Utc::now().timestamp_millis();
        Self {
            id: Uuid::new_v4(),
            volume_id,
            kind: kind.into(),
            state: OperationState::Accepted,
            items,
            error: None,
            created_at: at,
            updated_at: at,
            events: vec![OperationEvent {
                at,
                state: OperationState::Accepted,
            }],
        }
    }
    fn transition(&mut self, state: OperationState) {
        self.state = state;
        self.updated_at = chrono::Utc::now().timestamp_millis();
        self.events.push(OperationEvent {
            at: self.updated_at,
            state,
        });
    }
}

#[derive(Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Config {
    #[serde(default)]
    catalogs: BTreeMap<String, CatalogConfig>,
}
#[derive(Clone, Serialize, Deserialize)]
enum Task {
    Volume {
        request: VolumeRequest,
        root: WorkspaceId,
        lower: LayerId,
        head: LayerId,
        empty_checked: bool,
    },
    Fork(ForkRequest),
    Delete {
        workspace: WorkspaceId,
    },
}
#[derive(Clone, Serialize, Deserialize)]
struct Record {
    operation: Operation,
    catalog_ref: String,
    fingerprint: String,
    task: Task,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transport_fingerprint: Option<String>,
    #[serde(default)]
    claim_established: bool,
}
#[derive(Clone, Serialize, Deserialize)]
struct Binding {
    view: VolumeView,
    fingerprint: String,
}

struct Inner {
    pool: SqlitePool,
    config: Config,
    _lock: File,
    _catalog_locks: Vec<File>,
    gate: Mutex<()>,
    journal_id: Uuid,
    session_id: Uuid,
    connections: Mutex<BTreeMap<String, Arc<CatalogConnection>>>,
    scheduled: std::sync::Mutex<std::collections::HashSet<Uuid>>,
    admission: Arc<tokio::sync::Semaphore>,
    closing: std::sync::atomic::AtomicBool,
    stopping: tokio::sync::Notify,
    workers_idle: tokio::sync::Notify,
    #[cfg(test)]
    fail_after_effect: std::sync::atomic::AtomicI64,
    #[cfg(test)]
    fail_before_effect: std::sync::atomic::AtomicI64,
}
#[derive(Clone)]
pub struct Manager {
    inner: Arc<Inner>,
}
impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Manager { single_instance: true }")
    }
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
fn normalize_owner(owner: &mut Option<String>) -> Result<()> {
    if let Some(value) = owner {
        *value = value.trim().to_owned();
        if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
            return Err(invalid("owner must contain 1..256 printable bytes"));
        }
    }
    Ok(())
}
fn digest(value: &impl Serialize) -> Result<String> {
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(value).map_err(storage)?,
    )))
}
fn validate_catalog_file(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(invalid("catalog must be a regular file, not a symlink"));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() != 1 {
                    return Err(invalid("hard-linked catalog files are unsupported"));
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(storage(error)),
    }
    Ok(())
}

impl Manager {
    pub async fn open(state_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(state_dir).map_err(storage)?;
        let mut options = std::fs::OpenOptions::new();
        options.create(true).read(true).write(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options
            .open(state_dir.join("manager.lock"))
            .map_err(storage)?;
        lock.try_lock().map_err(|_| {
            ManagerError::new(
                "unavailable",
                "another Manager owns this state directory; multiple replicas are unsupported",
            )
        })?;
        let config: Config = match std::fs::read(state_dir.join("manager-catalogs.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|_| invalid("invalid local Manager catalog configuration"))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Config::default(),
            Err(error) => return Err(storage(error)),
        };
        let mut paths = std::collections::BTreeSet::new();
        let mut catalog_locks = Vec::new();
        for (name, cfg) in &config.catalogs {
            if !valid_name(name) {
                return Err(invalid("invalid configured catalog reference"));
            }
            cfg.validate()?;
            let Some(path) = cfg.sqlite_path() else {
                continue;
            };
            let parent = path.parent().unwrap().canonicalize().map_err(storage)?;
            if !paths.insert(parent.join(path.file_name().unwrap())) {
                return Err(invalid("catalog references must not alias the same file"));
            }
            let mut lock_name = path.as_os_str().to_os_string();
            lock_name.push(".manager.lock");
            let catalog_lock = options.open(PathBuf::from(lock_name)).map_err(storage)?;
            catalog_lock.try_lock().map_err(|_| {
                ManagerError::new(
                    "unavailable",
                    "another Manager owns a configured catalog; multiple replicas are unsupported",
                )
            })?;
            catalog_locks.push(catalog_lock);
        }
        let db_path = state_dir.join("manager.sqlite");
        options.open(&db_path).map_err(storage)?;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(db_path)
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .synchronous(SqliteSynchronous::Full),
            )
            .await
            .map_err(storage)?;
        sqlx::raw_sql("CREATE TABLE IF NOT EXISTS manager_operations (id TEXT PRIMARY KEY, scope TEXT NOT NULL, idem_key TEXT NOT NULL, digest TEXT NOT NULL, record TEXT NOT NULL, UNIQUE(scope,idem_key));
            CREATE TABLE IF NOT EXISTS manager_reservations (catalog_ref TEXT PRIMARY KEY, volume_id TEXT UNIQUE NOT NULL, operation_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS manager_volumes (id TEXT PRIMARY KEY, binding TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS manager_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), journal_id TEXT NOT NULL);")
            .execute(&pool).await.map_err(storage)?;
        sqlx::query("INSERT OR IGNORE INTO manager_identity VALUES(1,?)")
            .bind(Uuid::new_v4().to_string())
            .execute(&pool)
            .await
            .map_err(storage)?;
        let journal: String =
            sqlx::query_scalar("SELECT journal_id FROM manager_identity WHERE singleton=1")
                .fetch_one(&pool)
                .await
                .map_err(storage)?;
        let journal_id = Uuid::parse_str(&journal).map_err(storage)?;
        let manager = Self {
            inner: Arc::new(Inner {
                pool,
                config,
                _lock: lock,
                _catalog_locks: catalog_locks,
                gate: Mutex::new(()),
                journal_id,
                session_id: Uuid::new_v4(),
                connections: Mutex::new(BTreeMap::new()),
                scheduled: std::sync::Mutex::new(std::collections::HashSet::new()),
                admission: Arc::new(tokio::sync::Semaphore::new(64)),
                closing: std::sync::atomic::AtomicBool::new(false),
                stopping: tokio::sync::Notify::new(),
                workers_idle: tokio::sync::Notify::new(),
                #[cfg(test)]
                fail_after_effect: std::sync::atomic::AtomicI64::new(-1),
                #[cfg(test)]
                fail_before_effect: std::sync::atomic::AtomicI64::new(-1),
            }),
        };
        manager.start_renewal();
        let rows = sqlx::query("SELECT record FROM manager_operations")
            .fetch_all(&manager.inner.pool)
            .await
            .map_err(storage)?;
        for row in rows {
            let record: Record =
                serde_json::from_str(row.try_get("record").map_err(storage)?).map_err(storage)?;
            if matches!(
                record.operation.state,
                OperationState::Accepted
                    | OperationState::Running
                    | OperationState::RecoveryRequired
            ) {
                if manager
                    .inner
                    .config
                    .catalogs
                    .get(&record.catalog_ref)
                    .is_some_and(|config| config.sqlite_path().is_some())
                {
                    manager.execute(record.operation.id).await?;
                } else {
                    // Remote recovery must not block the local operation-query
                    // service while credentials or backend access are missing.
                    manager.schedule(record.operation.id);
                }
            }
        }
        Ok(manager)
    }

    fn fingerprint(&self, catalog_ref: &str) -> Result<String> {
        let config = self
            .inner
            .config
            .catalogs
            .get(catalog_ref)
            .ok_or_else(|| invalid("unknown configured catalog reference"))?;
        config.fingerprint()
    }
    fn catalog_journal_id(&self, catalog_ref: &str) -> Uuid {
        // Bind permanent authority to this journal's configured reference too.
        // Two aliases of one physical namespace must not create independent
        // renewal handles for the same controller session.
        let mut digest = Sha256::new();
        digest.update(b"brewfs/manager-catalog-owner/v1");
        digest.update(self.inner.journal_id.as_bytes());
        digest.update(catalog_ref.as_bytes());
        Uuid::from_bytes(digest.finalize()[..16].try_into().unwrap())
    }
    async fn connection(
        &self,
        catalog_ref: &str,
        allow_create: bool,
        native: bool,
    ) -> Result<Arc<CatalogConnection>> {
        if let Some(connection) = self
            .inner
            .connections
            .lock()
            .await
            .get(catalog_ref)
            .cloned()
        {
            #[cfg(feature = "native-packed-base")]
            if native != connection.native.is_some() {
                return Err(conflict(
                    "catalog reference already has a different volume profile",
                ));
            }
            return Ok(connection);
        }
        let cfg = self
            .inner
            .config
            .catalogs
            .get(catalog_ref)
            .ok_or_else(|| invalid("unknown configured catalog reference"))?;
        Ok(Arc::new(cfg.connect(allow_create, native).await?))
    }

    async fn claim_catalog(
        &self,
        catalog_ref: &str,
        native: bool,
        connection: Arc<CatalogConnection>,
        require_empty: bool,
        require_existing: bool,
    ) -> Result<Arc<CatalogConnection>> {
        if connection.raw_backend().is_none() {
            return Ok(connection);
        }
        let (connection, require_empty, require_existing) = if let Some(guard) = connection.guard()
        {
            if guard.renew().await.is_ok() {
                return Ok(connection);
            }
            // A renewal reply can be ambiguous, or the old session can have
            // expired. Re-read permanent authority; never recreate it here.
            self.inner.connections.lock().await.remove(catalog_ref);
            let cfg = &self.inner.config.catalogs[catalog_ref];
            (Arc::new(cfg.connect(false, native).await?), false, true)
        } else {
            (connection, require_empty, require_existing)
        };
        let mut connection =
            Arc::try_unwrap(connection).map_err(|_| storage("unclaimed connection was shared"))?;
        connection
            .claim(
                self.catalog_journal_id(catalog_ref),
                self.inner.session_id,
                MANAGER_SESSION_TTL_NS,
                require_empty,
                require_existing,
            )
            .await?;
        let connection = Arc::new(connection);
        let _ = native;
        self.inner
            .connections
            .lock()
            .await
            .insert(catalog_ref.to_owned(), connection.clone());
        Ok(connection)
    }

    #[cfg(test)]
    async fn catalog(&self, catalog_ref: &str, allow_create: bool) -> Result<SqliteWorkspaceStore> {
        let cfg = self
            .inner
            .config
            .catalogs
            .get(catalog_ref)
            .ok_or_else(|| invalid("unknown catalog"))?;
        let path = cfg
            .sqlite_path()
            .ok_or_else(|| invalid("test requires SQLite"))?;
        validate_catalog_file(path)?;
        let url = format!("sqlite://{}", path.display());
        if allow_create {
            SqliteWorkspaceStore::connect(&url).await
        } else {
            SqliteWorkspaceStore::connect_existing(&url).await
        }
        .map_err(catalog_error)
    }

    fn start_renewal(&self) {
        let weak = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                if inner.closing.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                let connections: Vec<_> =
                    inner.connections.lock().await.values().cloned().collect();
                drop(inner);
                for connection in connections {
                    if let Some(guard) = connection.guard()
                        && guard.renew().await.is_err()
                    {
                        tracing::warn!(
                            "Manager catalog session renewal failed; backend mutation guards remain authoritative"
                        );
                    }
                }
            }
        });
    }

    /// Release only this process's guarded catalog sessions. This never releases
    /// workspace writers or changes catalog/data ownership.
    pub async fn shutdown(&self) -> Result<()> {
        self.inner
            .closing
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.inner.admission.close();
        self.inner.stopping.notify_waiters();
        let gate = self.inner.gate.lock().await;
        let connections = std::mem::take(&mut *self.inner.connections.lock().await);
        let mut failure = None;
        for connection in connections.values() {
            if let Some(guard) = connection.guard()
                && let Err(error) = guard.release().await
            {
                failure.get_or_insert_with(|| catalog_error(error));
            }
            if let Err(error) = connection.shutdown().await {
                failure.get_or_insert(error);
            }
        }
        drop(gate);
        while !self.inner.scheduled.lock().unwrap().is_empty() {
            self.inner.workers_idle.notified().await;
        }
        failure.map_or(Ok(()), Err)
    }

    fn admit(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        if self.inner.closing.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(ManagerError::new(
                "unavailable",
                "Manager is shutting down; retry the original request after restart",
            ));
        }
        self.inner
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                ManagerError::new(
                    "overloaded",
                    "Manager admission limit reached; retry the original request and key",
                )
            })
    }
    async fn record(&self, id: Uuid) -> Result<Record> {
        let row = sqlx::query("SELECT record FROM manager_operations WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.inner.pool)
            .await
            .map_err(storage)?
            .ok_or_else(|| {
                ManagerError::new("not_found", "operation not found in this Manager scope")
            })?;
        serde_json::from_str(row.try_get("record").map_err(storage)?).map_err(storage)
    }
    async fn save(&self, record: &Record) -> Result<()> {
        sqlx::query("UPDATE manager_operations SET record=? WHERE id=?")
            .bind(serde_json::to_string(record).map_err(storage)?)
            .bind(record.operation.id.to_string())
            .execute(&self.inner.pool)
            .await
            .map_err(storage)?;
        Ok(())
    }
    fn schedule(&self, id: Uuid) {
        {
            let mut scheduled = self.inner.scheduled.lock().unwrap();
            if self.inner.closing.load(std::sync::atomic::Ordering::SeqCst) || !scheduled.insert(id)
            {
                return;
            }
        }
        let manager = self.clone();
        tokio::spawn(async move {
            let stopping = manager.inner.stopping.notified();
            tokio::pin!(stopping);
            stopping.as_mut().enable();
            for attempt in 0..8 {
                if manager
                    .inner
                    .closing
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    break;
                }
                if manager.execute(id).await.is_err() {
                    tracing::error!(operation_id=%id, "Manager operation journal unavailable; restart recovery required");
                    break;
                }
                let waiting = manager.operation(id).await.is_ok_and(|operation| {
                    operation.state == OperationState::RecoveryRequired
                        && operation
                            .error
                            .as_ref()
                            .is_some_and(|error| error.code == "session_busy")
                });
                if !waiting || attempt == 7 {
                    break;
                }
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {},
                    _ = &mut stopping => break,
                }
            }
            manager.inner.scheduled.lock().unwrap().remove(&id);
            manager.inner.workers_idle.notify_one();
        });
    }
    async fn replay(
        &self,
        scope: &str,
        key: &str,
        request_digest: &str,
    ) -> Result<Option<Operation>> {
        validate_key(key)?;
        let row = sqlx::query(
            "SELECT digest,record FROM manager_operations WHERE scope=? AND idem_key=?",
        )
        .bind(scope)
        .bind(key)
        .fetch_optional(&self.inner.pool)
        .await
        .map_err(storage)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let previous: String = row.try_get("digest").map_err(storage)?;
        if previous != request_digest {
            return Err(ManagerError::new(
                "idempotency_conflict",
                "Idempotency-Key already belongs to a different normalized request",
            ));
        }
        let mut record: Record =
            serde_json::from_str(row.try_get("record").map_err(storage)?).map_err(storage)?;
        if record.operation.state == OperationState::Failed {
            let _guard = self.inner.gate.lock().await;
            record = self.record(record.operation.id).await?;
            if record.operation.state == OperationState::Failed {
                // Acknowledging an explicit retry also requires durable admission:
                // a crash before the worker starts must not lose that retry.
                record.operation.error = None;
                record.operation.transition(OperationState::Accepted);
                self.save(&record).await?;
            }
        }
        Ok(Some(record.operation))
    }
    async fn prepare(
        &self,
        scope: &str,
        key: &str,
        request_digest: String,
        record: Record,
        reserve: bool,
    ) -> Result<Operation> {
        validate_key(key)?;
        let _guard = self.inner.gate.lock().await;
        let mut tx = self.inner.pool.begin().await.map_err(storage)?;
        if let Some(row) =
            sqlx::query("SELECT digest,record FROM manager_operations WHERE scope=? AND idem_key=?")
                .bind(scope)
                .bind(key)
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?
        {
            let existing: String = row.try_get("digest").map_err(storage)?;
            if existing != request_digest {
                return Err(ManagerError::new(
                    "idempotency_conflict",
                    "Idempotency-Key already belongs to a different normalized request",
                ));
            }
            let mut prior: Record =
                serde_json::from_str(row.try_get("record").map_err(storage)?).map_err(storage)?;
            if prior.operation.state == OperationState::Failed {
                prior.operation.error = None;
                prior.operation.transition(OperationState::Accepted);
                sqlx::query("UPDATE manager_operations SET record=? WHERE id=?")
                    .bind(serde_json::to_string(&prior).map_err(storage)?)
                    .bind(prior.operation.id.to_string())
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
            }
            tx.commit().await.map_err(storage)?;
            return Ok(prior.operation);
        }
        let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM manager_operations WHERE json_extract(record,'$.operation.state') IN ('accepted','running','recovery_required')")
            .fetch_one(&mut *tx).await.map_err(storage)?;
        if pending >= MAX_PENDING_OPERATIONS {
            return Err(ManagerError::new(
                "overloaded",
                "Manager pending operation limit reached; retry the original request and key",
            ));
        }
        if reserve {
            let exists = sqlx::query(
                "SELECT volume_id FROM manager_reservations WHERE catalog_ref=? OR volume_id=?",
            )
            .bind(&record.catalog_ref)
            .bind(record.operation.volume_id.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?;
            if exists.is_some() {
                return Err(conflict(
                    "catalog or volume is already reserved; reuse its original Idempotency-Key",
                ));
            }
            sqlx::query("INSERT INTO manager_reservations VALUES(?,?,?)")
                .bind(&record.catalog_ref)
                .bind(record.operation.volume_id.to_string())
                .bind(record.operation.id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
        }
        sqlx::query("INSERT INTO manager_operations VALUES(?,?,?,?,?)")
            .bind(record.operation.id.to_string())
            .bind(scope)
            .bind(key)
            .bind(request_digest)
            .bind(serde_json::to_string(&record).map_err(storage)?)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(record.operation)
    }
    pub async fn create_volume(&self, key: &str, mut request: VolumeRequest) -> Result<Operation> {
        let _admission = self.admit()?;
        request.catalog_ref = request.catalog_ref.trim().into();
        normalize_owner(&mut request.owner)?;
        let request_digest = digest(&request)?;
        if let Some(operation) = self.replay("volumes", key, &request_digest).await? {
            self.schedule(operation.id);
            return Ok(operation);
        }
        if !valid_name(&request.catalog_ref) {
            return Err(invalid("invalid catalog reference"));
        }
        let fingerprint = self.fingerprint(&request.catalog_ref)?;
        let native = match request.volume_format.as_str() {
            "workspace-v1" => false,
            "workspace-native-v2" if cfg!(feature = "native-packed-base") => true,
            _ => {
                return Err(ManagerError::new(
                    "unsupported",
                    "volume profile is unsupported by this Manager build",
                ));
            }
        };
        let cfg = &self.inner.config.catalogs[&request.catalog_ref];
        if native && cfg.sqlite_path().is_some() {
            return Err(ManagerError::new(
                "unsupported",
                "native-v2 requires Redis or TiKV",
            ));
        }
        let transport_fingerprint = Some(cfg.transport_fingerprint()?);
        match request.mode {
            VolumeMode::Initialize
                if !request.confirm_empty || request.expected_volume_id.is_some() =>
            {
                return Err(invalid(
                    "initialize requires confirm_empty=true and no expected_volume_id",
                ));
            }
            VolumeMode::Register
                if request.expected_volume_id.is_none()
                    || request.confirm_empty
                    || request.owner.is_some() =>
            {
                return Err(invalid(
                    "register requires expected_volume_id and does not accept owner or confirm_empty",
                ));
            }
            _ => {}
        }
        let volume = request.expected_volume_id.unwrap_or_else(Uuid::new_v4);
        let root = WorkspaceId::new();
        let head = LayerId::new();
        let mut item = OperationItem::new(
            (request.mode == VolumeMode::Initialize).then_some(root),
            (request.mode == VolumeMode::Initialize).then_some(head),
        );
        if native && request.mode == VolumeMode::Initialize {
            item.native_creation = Some(NativeCreation {
                domain_id: Uuid::new_v4(),
                owner_id: Uuid::new_v4(),
            });
        }
        let record = Record {
            operation: Operation::new(
                volume,
                if request.mode == VolumeMode::Initialize {
                    "initialize_volume"
                } else {
                    "register_volume"
                },
                vec![item],
            ),
            catalog_ref: request.catalog_ref.clone(),
            fingerprint,
            transport_fingerprint,
            claim_established: false,
            task: Task::Volume {
                request,
                root,
                lower: LayerId::new(),
                head,
                empty_checked: false,
            },
        };
        let operation = self
            .prepare("volumes", key, request_digest, record, true)
            .await?;
        self.schedule(operation.id);
        Ok(operation)
    }
    pub async fn fork(&self, volume: Uuid, key: &str, request: ForkRequest) -> Result<Operation> {
        self.submit_fork(volume, key, request, true).await
    }
    async fn submit_fork(
        &self,
        volume: Uuid,
        key: &str,
        mut request: ForkRequest,
        schedule: bool,
    ) -> Result<Operation> {
        let _admission = self.admit()?;
        normalize_owner(&mut request.owner)?;
        let request_digest = digest(&request)?;
        let scope = format!("volumes/{volume}/forks");
        if let Some(operation) = self.replay(&scope, key, &request_digest).await? {
            if schedule {
                self.schedule(operation.id);
            }
            return Ok(operation);
        }
        if !(1..=MAX_FORK_COUNT).contains(&request.count) {
            return Err(invalid("fork count must be 1..100"));
        }
        let binding = self.binding(volume).await?;
        Self::validate_revision_profile(&binding.view, &request.revision)?;
        let native = binding.view.volume_format == "workspace-native-v2";
        let items = (0..request.count)
            .map(|_| {
                let mut item = OperationItem::new(Some(WorkspaceId::new()), Some(LayerId::new()));
                if native {
                    item.native_creation = Some(NativeCreation {
                        domain_id: Uuid::new_v4(),
                        owner_id: Uuid::new_v4(),
                    });
                }
                item
            })
            .collect();
        let record = Record {
            operation: Operation::new(volume, "fork", items),
            catalog_ref: binding.view.catalog_ref,
            fingerprint: binding.fingerprint,
            transport_fingerprint: None,
            claim_established: true,
            task: Task::Fork(request),
        };
        let op = self
            .prepare(&scope, key, request_digest, record, false)
            .await?;
        if schedule {
            self.schedule(op.id);
        }
        Ok(op)
    }
    pub async fn delete_workspace(
        &self,
        volume: Uuid,
        workspace: WorkspaceId,
        key: &str,
    ) -> Result<Operation> {
        let _admission = self.admit()?;
        let scope = format!("volumes/{volume}/workspaces/{workspace}/delete");
        let request_digest = digest(&workspace)?;
        if let Some(operation) = self.replay(&scope, key, &request_digest).await? {
            self.schedule(operation.id);
            return Ok(operation);
        }
        let binding = self.binding(volume).await?;
        let record = Record {
            operation: Operation::new(
                volume,
                "delete_workspace",
                vec![OperationItem::new(Some(workspace), None)],
            ),
            catalog_ref: binding.view.catalog_ref,
            fingerprint: binding.fingerprint,
            transport_fingerprint: None,
            claim_established: true,
            task: Task::Delete { workspace },
        };
        let op = self
            .prepare(&scope, key, request_digest, record, false)
            .await?;
        self.schedule(op.id);
        Ok(op)
    }
    pub async fn operation(&self, id: Uuid) -> Result<Operation> {
        Ok(self.record(id).await?.operation)
    }
    async fn binding(&self, id: Uuid) -> Result<Binding> {
        let row = sqlx::query("SELECT binding FROM manager_volumes WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.inner.pool)
            .await
            .map_err(storage)?
            .ok_or_else(|| {
                ManagerError::new("not_found", "volume not registered in this Manager scope")
            })?;
        let binding: Binding =
            serde_json::from_str(row.try_get("binding").map_err(storage)?).map_err(storage)?;
        if self.fingerprint(&binding.view.catalog_ref).map_err(|_| {
            ManagerError::new(
                "recovery_required",
                "recorded catalog configuration is unavailable",
            )
        })? != binding.fingerprint
        {
            return Err(ManagerError::new(
                "recovery_required",
                "configured catalog identity changed; restore the original configuration",
            ));
        }
        Ok(binding)
    }
    async fn bound_catalog(&self, id: Uuid) -> Result<Arc<CatalogConnection>> {
        let binding = self.binding(id).await?;
        let store = self
            .connection(
                &binding.view.catalog_ref,
                false,
                binding.view.volume_format == "workspace-native-v2",
            )
            .await?;
        self.validate_binding_header(&store, &binding.view).await?;
        if let Some(raw) = store.raw_backend() {
            crate::workspace_overlay::stores::manager_guard::ManagerGuard::verify_owner(
                raw.as_ref(),
                self.catalog_journal_id(&binding.view.catalog_ref),
            )
            .await
            .map_err(catalog_error)?;
        }
        Ok(store)
    }
    async fn operation_catalog(&self, record: &Record) -> Result<Arc<CatalogConnection>> {
        let binding = self.binding(record.operation.volume_id).await?;
        if binding.view.catalog_ref != record.catalog_ref
            || binding.fingerprint != record.fingerprint
        {
            return Err(ManagerError::new(
                "recovery_required",
                "recorded volume binding changed; refusing to redirect an operation",
            ));
        }
        let native = binding.view.volume_format == "workspace-native-v2";
        let store = self.connection(&record.catalog_ref, false, native).await?;
        self.validate_binding_header(&store, &binding.view).await?;
        self.claim_catalog(&record.catalog_ref, native, store, false, true)
            .await
    }
    async fn header(&self, connection: &CatalogConnection, id: Uuid) -> Result<VolumeHeader> {
        connection.validate_schema().await?;
        let header = connection
            .store
            .load_volume_header()
            .await
            .map_err(catalog_error)?
            .ok_or_else(|| conflict("catalog has no initialized volume header"))?;
        admin::validate_volume_header(&header, id).map_err(catalog_error)?;
        if connection.has_packed_bindings().await? {
            return Err(ManagerError::new(
                "unsupported",
                "packed catalog bindings require a separate lifecycle implementation",
            ));
        }
        Ok(header)
    }
    async fn validate_binding_header(
        &self,
        connection: &CatalogConnection,
        view: &VolumeView,
    ) -> Result<()> {
        let catalog_header = self.header(connection, view.id).await?;
        if view.volume_format == "workspace-native-v2" {
            #[cfg(feature = "native-packed-base")]
            {
                let header = native::header(connection, &catalog_header).await?;
                if view.storage_namespace_id != Some(header.storage_namespace_id) {
                    return Err(conflict(
                        "native storage namespace differs from the recorded volume",
                    ));
                }
            }
            #[cfg(not(feature = "native-packed-base"))]
            return Err(ManagerError::new(
                "unsupported",
                "native-packed-base is not compiled into this Manager",
            ));
        } else {
            if connection.native_locator().await?.is_some() {
                return Err(ManagerError::new(
                    "unsupported",
                    "native catalog requires its explicit native volume profile",
                ));
            }
            if view.volume_format != catalog_header.volume_format
                || view.schema_version != catalog_header.schema_version
            {
                return Err(conflict("catalog profile differs from the recorded volume"));
            }
        }
        Ok(())
    }
    fn revision_for(view: &VolumeView, catalog_revision: BaseRevision) -> Result<Revision> {
        if view.volume_format == "workspace-native-v2" {
            Ok(Revision::Native(NativeCatalogRevision {
                kind: NativeRevisionKind::Catalog,
                volume_id: view.id,
                storage_namespace_id: view
                    .storage_namespace_id
                    .ok_or_else(|| conflict("native volume identity is missing"))?,
                catalog_revision,
            }))
        } else {
            Ok(Revision::Workspace(catalog_revision))
        }
    }
    fn validate_revision_profile(view: &VolumeView, revision: &Revision) -> Result<()> {
        match (view.volume_format.as_str(), revision) {
            ("workspace-v1", Revision::Workspace(_)) => Ok(()),
            ("workspace-native-v2", Revision::Native(native))
                if native.volume_id == view.id
                    && Some(native.storage_namespace_id) == view.storage_namespace_id =>
            {
                Ok(())
            }
            _ => Err(conflict(
                "immutable revision does not match the volume profile and identity",
            )),
        }
    }
    pub async fn list_volumes(&self) -> Result<Vec<VolumeView>> {
        let rows = sqlx::query("SELECT binding FROM manager_volumes ORDER BY id")
            .fetch_all(&self.inner.pool)
            .await
            .map_err(storage)?;
        let mut volumes = Vec::with_capacity(rows.len());
        for row in rows {
            let binding: Binding =
                serde_json::from_str(row.try_get("binding").map_err(storage)?).map_err(storage)?;
            volumes.push(self.inspect_volume(binding.view.id).await?);
        }
        Ok(volumes)
    }
    pub async fn inspect_volume(&self, id: Uuid) -> Result<VolumeView> {
        let binding = self.binding(id).await?;
        self.bound_catalog(id).await?;
        Ok(binding.view)
    }
    pub async fn list_workspaces(&self, volume: Uuid) -> Result<Vec<WorkspaceRecord>> {
        self.bound_catalog(volume)
            .await?
            .store
            .list_workspaces()
            .await
            .map_err(catalog_error)
    }
    pub async fn inspect_workspace(&self, volume: Uuid, id: WorkspaceId) -> Result<WorkspaceView> {
        let binding = self.binding(volume).await?;
        let connection = self.bound_catalog(volume).await?;
        let view = admin::inspect_workspace(connection.store.as_ref(), id)
            .await
            .map_err(catalog_error)?;
        let native = if binding.view.volume_format == "workspace-native-v2" {
            if view.record.upper_bucket.is_some() {
                return Err(ManagerError::new(
                    "unsupported",
                    "native P1 does not route per-workspace upper buckets",
                ));
            }
            #[cfg(feature = "native-packed-base")]
            {
                let header =
                    native::header(&connection, &self.header(&connection, volume).await?).await?;
                Some(native::inspect(&connection, &header, id).await?)
            }
            #[cfg(not(feature = "native-packed-base"))]
            return Err(ManagerError::new(
                "unsupported",
                "native-packed-base is not compiled into this Manager",
            ));
        } else {
            None
        };
        Ok(WorkspaceView {
            mount_identity: MountIdentity {
                catalog_ref: binding.view.catalog_ref.clone(),
                metadata_backend: connection.backend_kind().into(),
                volume_format: binding.view.volume_format.clone(),
                volume_id: volume,
                workspace_id: id,
                workspace_namespace: connection.namespace().map(str::to_owned),
                storage_namespace_id: binding.view.storage_namespace_id,
                upper_bucket: view.record.upper_bucket.clone(),
            },
            record: view.record,
            base_revision: Self::revision_for(&binding.view, view.base_revision)?,
            layer_depth: view.layer_depth,
            leases: view.leases,
            native,
        })
    }
    pub async fn inspect_revision(&self, volume: Uuid, layer: LayerId) -> Result<Revision> {
        let binding = self.binding(volume).await?;
        let record = self
            .bound_catalog(volume)
            .await?
            .store
            .load_layer(layer)
            .await
            .map_err(catalog_error)?;
        Self::revision_for(
            &binding.view,
            admin::revision_from_layer(&record).map_err(catalog_error)?,
        )
    }

    async fn execute(&self, id: Uuid) -> Result<()> {
        let _guard = self.inner.gate.lock().await;
        if self.inner.closing.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        let mut record = self.record(id).await?;
        if record.operation.state == OperationState::Succeeded {
            return Ok(());
        }
        record.operation.error = None;
        record.operation.transition(OperationState::Running);
        self.save(&record).await?;
        for index in 0..record.operation.items.len() {
            if record.operation.items[index].state == ItemState::Succeeded {
                continue;
            }
            let outcome = self.effect(&mut record, index).await;
            #[cfg(test)]
            if outcome.is_ok()
                && self
                    .inner
                    .fail_after_effect
                    .compare_exchange(
                        index as i64,
                        -1,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    )
                    .is_ok()
            {
                // Emulate a lost backend reply before recording this item's success.
                record
                    .operation
                    .transition(OperationState::RecoveryRequired);
                self.save(&record).await?;
                return Ok(());
            }
            match outcome {
                Ok(()) => {
                    let item = &mut record.operation.items[index];
                    item.state = ItemState::Succeeded;
                    item.error = None;
                    item.deletion_marked = matches!(record.task, Task::Delete { .. });
                    item.metadata_ready = !item.deletion_marked;
                }
                Err(error) => {
                    let recover = matches!(
                        error.code,
                        "unavailable" | "recovery_required" | "session_busy"
                    );
                    // A rejected request must not permanently occupy an untouched
                    // catalog. Uncertain effects and initialized schemas retain ownership.
                    let untouched = matches!(&record.task,Task::Volume {request,..} if request.mode==VolumeMode::Register)
                        || matches!(
                            &record.task,
                            Task::Volume {
                                empty_checked: false,
                                ..
                            }
                        );
                    if !recover && untouched && !record.claim_established {
                        sqlx::query("DELETE FROM manager_reservations WHERE operation_id=? AND NOT EXISTS (SELECT 1 FROM manager_volumes WHERE id=?)")
                            .bind(record.operation.id.to_string())
                            .bind(record.operation.volume_id.to_string())
                            .execute(&self.inner.pool)
                            .await
                            .map_err(storage)?;
                    }
                    let detail = OperationError {
                        code: error.code.into(),
                        message: error.message,
                    };
                    record.operation.items[index].state = if recover {
                        ItemState::RecoveryRequired
                    } else {
                        ItemState::Failed
                    };
                    record.operation.items[index].error = Some(detail.clone());
                    record.operation.error = Some(detail);
                    record.operation.transition(if recover {
                        OperationState::RecoveryRequired
                    } else {
                        OperationState::Failed
                    });
                    self.save(&record).await?;
                    return Ok(());
                }
            }
            self.save(&record).await?;
        }
        record.operation.transition(OperationState::Succeeded);
        self.save(&record).await
    }
    async fn effect(&self, record: &mut Record, index: usize) -> Result<()> {
        #[cfg(test)]
        if self
            .inner
            .fail_before_effect
            .compare_exchange(
                index as i64,
                -1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
        {
            return Err(ManagerError::new(
                "recovery_required",
                "injected catalog interruption before mutation",
            ));
        }
        if self.fingerprint(&record.catalog_ref).map_err(|_| {
            ManagerError::new(
                "recovery_required",
                "recorded catalog configuration is unavailable",
            )
        })? != record.fingerprint
        {
            return Err(ManagerError::new(
                "recovery_required",
                "configured catalog identity changed; restore the original configuration",
            ));
        }
        match record.task.clone() {
            Task::Volume {
                request,
                root,
                lower,
                head,
                empty_checked,
            } => {
                // A previous definitive validation failure may have relinquished
                // the reservation. Replaying that key cannot take another operation's scope.
                let mut tx = self.inner.pool.begin().await.map_err(storage)?;
                if let Some(row)=sqlx::query("SELECT operation_id FROM manager_reservations WHERE catalog_ref=? OR volume_id=?")
                    .bind(&record.catalog_ref).bind(record.operation.volume_id.to_string()).fetch_optional(&mut *tx).await.map_err(storage)? {
                    let owner:String=row.try_get("operation_id").map_err(storage)?;
                    if owner!=record.operation.id.to_string() { return Err(conflict("catalog is reserved by another operation")); }
                } else {
                    sqlx::query("INSERT INTO manager_reservations VALUES(?,?,?)").bind(&record.catalog_ref).bind(record.operation.volume_id.to_string())
                        .bind(record.operation.id.to_string()).execute(&mut *tx).await.map_err(storage)?;
                }
                tx.commit().await.map_err(storage)?;
                let is_native = request.volume_format == "workspace-native-v2";
                let mut connection = self
                    .connection(
                        &record.catalog_ref,
                        request.mode == VolumeMode::Initialize && !empty_checked,
                        is_native,
                    )
                    .await
                    .map_err(|error| {
                        if request.mode == VolumeMode::Initialize
                            && empty_checked
                            && error.code == "not_found"
                        {
                            ManagerError::new(
                                "recovery_required",
                                "original initialization catalog is missing; restore it before retrying",
                            )
                        } else {
                            error
                        }
                    })?;
                if !is_native && connection.native_locator().await?.is_some() {
                    return Err(ManagerError::new(
                        "unsupported",
                        "native catalog requires its explicit native volume profile",
                    ));
                }
                if request.mode == VolumeMode::Initialize {
                    #[cfg(feature = "native-packed-base")]
                    if is_native {
                        native::check_initialize_target(&connection, record.operation.volume_id)
                            .await?;
                    }
                    let existing = connection
                        .store
                        .load_volume_header()
                        .await
                        .map_err(catalog_error)?;
                    if let Some(header) = &existing {
                        admin::validate_volume_header(header, record.operation.volume_id)
                            .map_err(catalog_error)?;
                    } else if record
                        .transport_fingerprint
                        .as_ref()
                        .is_some_and(|original| {
                            self.inner.config.catalogs[&record.catalog_ref]
                                .transport_fingerprint()
                                .as_ref()
                                .ok()
                                != Some(original)
                        })
                    {
                        return Err(ManagerError::new(
                            "recovery_required",
                            "initialization route changed without the recorded volume identity; restore the original catalog route",
                        ));
                    }
                    if !empty_checked
                        && self.inner.config.catalogs[&record.catalog_ref]
                            .sqlite_path()
                            .is_some()
                    {
                        // Inspect every user object, including legacy/foreign metadata, before schema creation.
                        let cfg = &self.inner.config.catalogs[&record.catalog_ref];
                        let probe = SqlitePoolOptions::new()
                            .max_connections(1)
                            .connect_with(
                                SqliteConnectOptions::new()
                                    .filename(cfg.sqlite_path().unwrap())
                                    .read_only(true),
                            )
                            .await
                            .map_err(storage)?;
                        let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE substr(name,1,7) != 'sqlite_'").fetch_one(&probe).await.map_err(storage)?;
                        probe.close().await;
                        if count != 0 {
                            return Err(conflict(
                                "initialize requires an empty SQLite namespace; use register for existing volumes",
                            ));
                        }
                        if let Task::Volume { empty_checked, .. } = &mut record.task {
                            *empty_checked = true;
                        }
                        // Persist proof of the empty namespace before creating any catalog tables.
                        self.save(record).await?;
                    }
                    connection = self
                        .claim_catalog(
                            &record.catalog_ref,
                            is_native,
                            connection,
                            !record.claim_established,
                            record.claim_established,
                        )
                        .await?;
                    if connection.guard().is_some() && !record.claim_established {
                        record.claim_established = true;
                        // A crash may leave only the permanent claim. Never recreate a
                        // missing claim after this checkpoint, even on an empty target.
                        self.save(record).await?;
                    }
                    if connection
                        .store
                        .load_volume_header()
                        .await
                        .map_err(catalog_error)?
                        .is_none()
                    {
                        connection
                            .initialize_empty(CreateVolumeRoot {
                                upper_bucket: None,
                                volume_format: "workspace-v1".into(),
                                schema_version: WORKSPACE_SCHEMA_VERSION,
                                volume_id: record.operation.volume_id,
                                workspace_id: root,
                                root_layer_id: lower,
                                writable_layer_id: head,
                                owner_id: request.owner.clone(),
                            })
                            .await?;
                    }
                }
                let header = self.header(&connection, record.operation.volume_id).await?;
                #[cfg(feature = "native-packed-base")]
                let native_header = if is_native {
                    Some(if request.mode == VolumeMode::Initialize {
                        native::initialize(&connection, record.operation.volume_id).await?
                    } else {
                        native::header(&connection, &header).await?
                    })
                } else {
                    None
                };
                if request.mode == VolumeMode::Register {
                    // Validate before first claiming an externally initialized catalog.
                    for workspace in connection
                        .store
                        .list_workspaces()
                        .await
                        .map_err(catalog_error)?
                        .iter()
                        .filter(|workspace| workspace.state == WorkspaceState::Active)
                    {
                        admin::inspect_workspace(connection.store.as_ref(), workspace.workspace_id)
                            .await
                            .map_err(catalog_error)?;
                        #[cfg(feature = "native-packed-base")]
                        if let Some(native_header) = &native_header {
                            if workspace.upper_bucket.is_some() {
                                return Err(ManagerError::new(
                                    "unsupported",
                                    "native P1 does not route per-workspace upper buckets",
                                ));
                            }
                            native::inspect(&connection, native_header, workspace.workspace_id)
                                .await?;
                        }
                    }
                    connection = self
                        .claim_catalog(
                            &record.catalog_ref,
                            is_native,
                            connection,
                            false,
                            record.claim_established,
                        )
                        .await?;
                    if connection.guard().is_some() && !record.claim_established {
                        record.claim_established = true;
                        self.save(record).await?;
                    }
                    self.header(&connection, record.operation.volume_id).await?;
                    #[cfg(feature = "native-packed-base")]
                    if is_native {
                        native::header(&connection, &header).await?;
                    }
                }
                let root_revision = if request.mode == VolumeMode::Initialize {
                    #[cfg(feature = "native-packed-base")]
                    if let Some(native_header) = &native_header {
                        let creation = record.operation.items[index]
                            .native_creation
                            .as_ref()
                            .ok_or_else(|| {
                                conflict("native creation identity is missing from the journal")
                            })?;
                        native::ensure(&connection, native_header, root, creation).await?;
                    }
                    let view = admin::inspect_workspace(connection.store.as_ref(), root)
                        .await
                        .map_err(catalog_error)?;
                    if view.record.head_layer_id != head
                        || view.record.owner_id != request.owner
                        || view.base_revision.layer_id != lower
                        || view.record.head_epoch != 0
                        || view.record.state != WorkspaceState::Active
                        || view.record.upper_bucket.is_some()
                    {
                        return Err(conflict(
                            "initialized catalog does not match recorded resource identities",
                        ));
                    }
                    Some(view.base_revision)
                } else {
                    None
                };
                let mut binding = Binding {
                    view: VolumeView {
                        id: header.volume_id,
                        catalog_ref: record.catalog_ref.clone(),
                        volume_format: request.volume_format,
                        schema_version: header.schema_version,
                        metadata_ready: true,
                        root_workspace_id: (request.mode == VolumeMode::Initialize).then_some(root),
                        root_revision: None,
                        remote_ready: None,
                        metadata_backend: Some(connection.backend_kind().into()),
                        storage_namespace_id: None,
                        native_control_version: None,
                    },
                    fingerprint: record.fingerprint.clone(),
                };
                #[cfg(feature = "native-packed-base")]
                if let Some(native_header) = &native_header {
                    binding.view.schema_version = native_header.schema_version;
                    binding.view.storage_namespace_id = Some(native_header.storage_namespace_id);
                    binding.view.native_control_version =
                        Some(native_header.native_control_version);
                }
                binding.view.root_revision = root_revision
                    .map(|revision| Self::revision_for(&binding.view, revision))
                    .transpose()?;
                sqlx::query("INSERT INTO manager_volumes(id,binding) VALUES(?,?) ON CONFLICT(id) DO NOTHING")
                    .bind(header.volume_id.to_string()).bind(serde_json::to_string(&binding).map_err(storage)?).execute(&self.inner.pool).await.map_err(storage)?;
                let existing = self.binding(header.volume_id).await?;
                if existing.view.catalog_ref != binding.view.catalog_ref
                    || existing.fingerprint != binding.fingerprint
                    || existing.view.root_workspace_id != binding.view.root_workspace_id
                    || existing.view.volume_format != binding.view.volume_format
                    || existing.view.storage_namespace_id != binding.view.storage_namespace_id
                {
                    return Err(conflict(
                        "volume ID is already bound to a different catalog identity",
                    ));
                }
            }
            Task::Fork(request) => {
                let binding = self.binding(record.operation.volume_id).await?;
                Self::validate_revision_profile(&binding.view, &request.revision)?;
                let connection = self.operation_catalog(record).await?;
                admin::verify_revision(
                    connection.store.as_ref(),
                    request.revision.catalog_revision(),
                )
                .await
                .map_err(catalog_error)?;
                let item = &record.operation.items[index];
                #[cfg(feature = "native-packed-base")]
                if binding.view.volume_format == "workspace-native-v2" {
                    let header = native::header(
                        &connection,
                        &self.header(&connection, record.operation.volume_id).await?,
                    )
                    .await?;
                    let creation = item.native_creation.as_ref().ok_or_else(|| {
                        conflict("native creation identity is missing from the journal")
                    })?;
                    native::ensure(&connection, &header, item.workspace_id.unwrap(), creation)
                        .await?;
                }
                admin::ensure_workspace(connection.store.as_ref(), CreateWorkspace {workspace_id:item.workspace_id.unwrap(),head_layer_id:item.head_layer_id.unwrap(),base_revision:request.revision.catalog_revision().clone(),owner_id:request.owner,upper_bucket:None})
                    .await.map_err(|error| match error {
                        WorkspaceError::Conflict(_) => ManagerError::new("recovery_required", "recorded fork identity cannot be reconciled; retain the journal and inspect the catalog"),
                        other => catalog_error(other),
                    })?;
            }
            Task::Delete { workspace } => {
                let connection = self.operation_catalog(record).await?;
                #[cfg(feature = "native-packed-base")]
                let native_header = if self
                    .binding(record.operation.volume_id)
                    .await?
                    .view
                    .volume_format
                    == "workspace-native-v2"
                {
                    let header = native::header(
                        &connection,
                        &self.header(&connection, record.operation.volume_id).await?,
                    )
                    .await?;
                    let row = connection
                        .store
                        .load_workspace(workspace)
                        .await
                        .map_err(catalog_error)?;
                    if row.upper_bucket.is_some() {
                        return Err(ManagerError::new(
                            "unsupported",
                            "native P1 does not route per-workspace upper buckets",
                        ));
                    }
                    let view = native::inspect(&connection, &header, workspace).await?;
                    if view
                        .get("writer_lease_present")
                        .and_then(serde_json::Value::as_bool)
                        != Some(false)
                    {
                        return Err(conflict(
                            "native workspace has an active or unrecognized writer lease",
                        ));
                    }
                    Some(header)
                } else {
                    None
                };
                admin::mark_workspace_deleting(connection.store.as_ref(), workspace, false)
                    .await
                    .map_err(catalog_error)?;
                #[cfg(feature = "native-packed-base")]
                if let Some(header) = native_header {
                    // The catalog marker is already durable. A late writer or
                    // uncertain native reply is a partial operation, never an
                    // assertion that deletion had no effect.
                    native::mark_deleting(&connection, &header, workspace).await.map_err(|_| ManagerError::new("recovery_required", "catalog deletion is marked; native fencing requires reconciliation with the original operation"))?;
                }
            }
        }
        Ok(())
    }
}
