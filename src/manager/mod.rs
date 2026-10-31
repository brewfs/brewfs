//! Single-instance, durable lifecycle coordinator. Catalogs remain authoritative.

use crate::workspace_overlay::{
    admin,
    catalog::{CreateVolumeRoot, CreateWorkspace, WorkspaceStore},
    error::WorkspaceError,
    ids::{LayerId, WorkspaceId},
    model::{
        BaseRevision, VolumeHeader, WORKSPACE_SCHEMA_VERSION, WorkspaceRecord, WorkspaceState,
    },
    stores::database::SqliteWorkspaceStore,
};
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

#[cfg(test)]
mod binding_tests;
#[cfg(test)]
mod tests;

pub const MAX_FORK_COUNT: usize = 100;

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
    pub revision: BaseRevision,
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
    pub root_revision: Option<BaseRevision>,
    /// No remote durability or mount evidence is produced by this coordinator.
    pub remote_ready: Option<bool>,
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
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogConfig {
    backend: String,
    path: PathBuf,
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
            if cfg.backend != "sqlite" {
                return Err(ManagerError::new(
                    "unsupported",
                    "only the SQLite catalog backend is supported by this Manager",
                ));
            }
            if !valid_name(name) || !cfg.path.is_absolute() || cfg.path.file_name().is_none() {
                return Err(invalid(
                    "catalog references require safe names and absolute SQLite file paths",
                ));
            }
            if cfg
                .path
                .to_str()
                .is_none_or(|path| path.contains(['?', '#', '%']))
            {
                return Err(invalid(
                    "catalog paths must be UTF-8 without URL escape characters",
                ));
            }
            validate_catalog_file(&cfg.path)?;
            let parent = cfg
                .path
                .parent()
                .ok_or_else(|| invalid("invalid catalog path"))?
                .canonicalize()
                .map_err(storage)?;
            if !paths.insert(parent.join(cfg.path.file_name().unwrap())) {
                return Err(invalid("catalog references must not alias the same file"));
            }
            let mut lock_name = cfg.path.as_os_str().to_os_string();
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
            CREATE TABLE IF NOT EXISTS manager_volumes (id TEXT PRIMARY KEY, binding TEXT NOT NULL);")
            .execute(&pool).await.map_err(storage)?;
        let manager = Self {
            inner: Arc::new(Inner {
                pool,
                config,
                _lock: lock,
                _catalog_locks: catalog_locks,
                gate: Mutex::new(()),
                #[cfg(test)]
                fail_after_effect: std::sync::atomic::AtomicI64::new(-1),
                #[cfg(test)]
                fail_before_effect: std::sync::atomic::AtomicI64::new(-1),
            }),
        };
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
                manager.execute(record.operation.id).await?;
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
        let path = config
            .path
            .parent()
            .unwrap()
            .canonicalize()
            .map_err(storage)?
            .join(config.path.file_name().unwrap());
        digest(&path)
    }
    async fn catalog(&self, catalog_ref: &str, allow_create: bool) -> Result<SqliteWorkspaceStore> {
        let cfg = self
            .inner
            .config
            .catalogs
            .get(catalog_ref)
            .ok_or_else(|| invalid("unknown configured catalog reference"))?;
        validate_catalog_file(&cfg.path)?;
        if !allow_create && !cfg.path.is_file() {
            return Err(ManagerError::new(
                "not_found",
                "configured catalog file does not exist",
            ));
        }
        // URLs and credentials are not accepted by the HTTP API or stored in the journal.
        let path = cfg
            .path
            .to_str()
            .ok_or_else(|| invalid("catalog path must be UTF-8"))?;
        if path.contains(['?', '#', '%']) {
            return Err(invalid("catalog path contains reserved URL characters"));
        }
        let url = format!("sqlite://{path}");
        if allow_create {
            SqliteWorkspaceStore::connect(&url).await
        } else {
            SqliteWorkspaceStore::connect_existing(&url).await
        }
        .map_err(catalog_error)
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
        let manager = self.clone();
        tokio::spawn(async move {
            if manager.execute(id).await.is_err() {
                tracing::error!(operation_id=%id, "Manager operation journal unavailable; restart recovery required");
            }
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
        if request.volume_format != "workspace-v1" {
            return Err(ManagerError::new(
                "unsupported",
                "only workspace-v1 SQLite catalogs are supported",
            ));
        }
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
        let item = OperationItem::new(
            (request.mode == VolumeMode::Initialize).then_some(root),
            (request.mode == VolumeMode::Initialize).then_some(head),
        );
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
        let items = (0..request.count)
            .map(|_| OperationItem::new(Some(WorkspaceId::new()), Some(LayerId::new())))
            .collect();
        let record = Record {
            operation: Operation::new(volume, "fork", items),
            catalog_ref: binding.view.catalog_ref,
            fingerprint: binding.fingerprint,
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
    async fn bound_catalog(&self, id: Uuid) -> Result<SqliteWorkspaceStore> {
        let binding = self.binding(id).await?;
        let store = self.catalog(&binding.view.catalog_ref, false).await?;
        self.header(&store, id).await?;
        Ok(store)
    }
    async fn operation_catalog(&self, record: &Record) -> Result<SqliteWorkspaceStore> {
        let binding = self.binding(record.operation.volume_id).await?;
        if binding.view.catalog_ref != record.catalog_ref
            || binding.fingerprint != record.fingerprint
        {
            return Err(ManagerError::new(
                "recovery_required",
                "recorded volume binding changed; refusing to redirect an operation",
            ));
        }
        let store = self.catalog(&record.catalog_ref, false).await?;
        self.header(&store, record.operation.volume_id).await?;
        Ok(store)
    }
    async fn header(&self, store: &SqliteWorkspaceStore, id: Uuid) -> Result<VolumeHeader> {
        store
            .validate_manager_catalog_schema()
            .await
            .map_err(catalog_error)?;
        let header = store
            .load_volume_header()
            .await
            .map_err(catalog_error)?
            .ok_or_else(|| conflict("catalog has no initialized volume header"))?;
        admin::validate_volume_header(&header, id).map_err(catalog_error)?;
        if store
            .has_packed_catalog_bindings()
            .await
            .map_err(catalog_error)?
        {
            return Err(ManagerError::new(
                "unsupported",
                "packed catalog bindings require a separate lifecycle implementation",
            ));
        }
        Ok(header)
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
            .list_workspaces()
            .await
            .map_err(catalog_error)
    }
    pub async fn inspect_workspace(
        &self,
        volume: Uuid,
        id: WorkspaceId,
    ) -> Result<admin::WorkspaceView> {
        admin::inspect_workspace(&self.bound_catalog(volume).await?, id)
            .await
            .map_err(catalog_error)
    }
    pub async fn inspect_revision(&self, volume: Uuid, layer: LayerId) -> Result<BaseRevision> {
        let record = self
            .bound_catalog(volume)
            .await?
            .load_layer(layer)
            .await
            .map_err(catalog_error)?;
        admin::revision_from_layer(&record).map_err(catalog_error)
    }

    async fn execute(&self, id: Uuid) -> Result<()> {
        let _guard = self.inner.gate.lock().await;
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
                    let recover = matches!(error.code, "unavailable" | "recovery_required");
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
                    if !recover && untouched {
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
                let store = self
                    .catalog(
                        &record.catalog_ref,
                        request.mode == VolumeMode::Initialize && !empty_checked,
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
                if request.mode == VolumeMode::Initialize {
                    if !empty_checked {
                        // Inspect every user object, including legacy/foreign metadata, before schema creation.
                        let cfg = &self.inner.config.catalogs[&record.catalog_ref];
                        let probe = SqlitePoolOptions::new()
                            .max_connections(1)
                            .connect_with(
                                SqliteConnectOptions::new()
                                    .filename(&cfg.path)
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
                    if store
                        .load_volume_header()
                        .await
                        .map_err(catalog_error)?
                        .is_none()
                    {
                        store
                            .create_volume_root_in_empty_namespace(CreateVolumeRoot {
                                upper_bucket: None,
                                volume_format: "workspace-v1".into(),
                                schema_version: WORKSPACE_SCHEMA_VERSION,
                                volume_id: record.operation.volume_id,
                                workspace_id: root,
                                root_layer_id: lower,
                                writable_layer_id: head,
                                owner_id: request.owner.clone(),
                            })
                            .await
                            .map_err(catalog_error)?;
                    }
                }
                let header = self.header(&store, record.operation.volume_id).await?;
                let root_revision = if request.mode == VolumeMode::Initialize {
                    let view = admin::inspect_workspace(&store, root)
                        .await
                        .map_err(catalog_error)?;
                    if view.record.head_layer_id != head
                        || view.record.owner_id != request.owner
                        || view.base_revision.layer_id != lower
                        || view.record.head_epoch != 0
                        || view.record.state != WorkspaceState::Active
                    {
                        return Err(conflict(
                            "initialized catalog does not match recorded resource identities",
                        ));
                    }
                    Some(view.base_revision)
                } else {
                    // A header alone is not sufficient to register an invalid or unsupported catalog.
                    let workspaces = store.list_workspaces().await.map_err(catalog_error)?;
                    for workspace in workspaces
                        .iter()
                        .filter(|w| w.state == WorkspaceState::Active)
                    {
                        admin::inspect_workspace(&store, workspace.workspace_id)
                            .await
                            .map_err(catalog_error)?;
                    }
                    None
                };
                let binding = Binding {
                    view: VolumeView {
                        id: header.volume_id,
                        catalog_ref: record.catalog_ref.clone(),
                        volume_format: header.volume_format,
                        schema_version: header.schema_version,
                        metadata_ready: true,
                        root_workspace_id: (request.mode == VolumeMode::Initialize).then_some(root),
                        root_revision,
                        remote_ready: None,
                    },
                    fingerprint: record.fingerprint.clone(),
                };
                sqlx::query("INSERT INTO manager_volumes(id,binding) VALUES(?,?) ON CONFLICT(id) DO NOTHING")
                    .bind(header.volume_id.to_string()).bind(serde_json::to_string(&binding).map_err(storage)?).execute(&self.inner.pool).await.map_err(storage)?;
                let existing = self.binding(header.volume_id).await?;
                if existing.view.catalog_ref != binding.view.catalog_ref
                    || existing.fingerprint != binding.fingerprint
                    || existing.view.root_workspace_id != binding.view.root_workspace_id
                {
                    return Err(conflict(
                        "volume ID is already bound to a different catalog identity",
                    ));
                }
            }
            Task::Fork(request) => {
                let store = self.operation_catalog(record).await?;
                admin::verify_revision(&store, &request.revision)
                    .await
                    .map_err(catalog_error)?;
                let item = &record.operation.items[index];
                crate::workspace_overlay::lifecycle::WorkspaceLifecycle::new(Arc::new(store)).ensure_fork(CreateWorkspace {workspace_id:item.workspace_id.unwrap(),head_layer_id:item.head_layer_id.unwrap(),base_revision:request.revision,owner_id:request.owner,upper_bucket:None})
                    .await.map_err(|error| match error {
                        WorkspaceError::Conflict(_) => ManagerError::new("recovery_required", "recorded fork identity cannot be reconciled; retain the journal and inspect the catalog"),
                        other => catalog_error(other),
                    })?;
            }
            Task::Delete { workspace } => {
                let store = self.operation_catalog(record).await?;
                admin::mark_workspace_deleting(&store, workspace, false)
                    .await
                    .map_err(catalog_error)?;
            }
        }
        Ok(())
    }
}
