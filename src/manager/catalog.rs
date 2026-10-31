//! Local connection configuration and fenced catalog handles.
//!
//! Connection settings are never serialized into the operation journal or API.
//! Scope fingerprints exclude rotatable credentials and service addresses.

use super::{ManagerError, Result, catalog_error, digest, invalid, storage, validate_catalog_file};
use crate::workspace_overlay::{
    catalog::{CreateVolumeRoot, WorkspaceStore},
    model::WorkspaceRecord,
    packed_v3::wire005::V3MountBudget,
    stores::{
        database::SqliteWorkspaceStore,
        kv_backend::{KvReadLimits, WorkspaceKvBackend},
        kv_store::KvWorkspaceStore,
        manager_guard::{ManagerGuard, ManagerGuardedBackend},
        redis::RedisWorkspaceBackend,
        tikv::TiKvWorkspaceBackend,
    },
};
use serde::Deserialize;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

#[derive(Clone, Deserialize)]
#[serde(tag = "backend", rename_all = "lowercase", deny_unknown_fields)]
pub(super) enum CatalogConfig {
    Sqlite {
        path: PathBuf,
    },
    Redis {
        #[serde(default)]
        url: Option<String>,
        #[serde(default)]
        url_env: Option<String>,
        namespace: String,
        stable_cluster_id: String,
    },
    Tikv {
        pd_endpoints: Vec<String>,
        namespace: String,
        stable_cluster_id: String,
        #[serde(default)]
        tls: Option<TlsFiles>,
    },
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TlsFiles {
    ca_path: PathBuf,
    cert_path: PathBuf,
    key_path: PathBuf,
}

impl std::fmt::Debug for CatalogConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogConfig")
            .field("backend", &self.backend_kind())
            .field("connection", &"[REDACTED]")
            .finish()
    }
}

fn safe_identity(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn safe_env_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn unsupported(message: &str) -> ManagerError {
    ManagerError::new("unsupported", message)
}

impl CatalogConfig {
    pub(super) fn backend_kind(&self) -> &'static str {
        match self {
            Self::Sqlite { .. } => "sqlite",
            Self::Redis { .. } => "redis",
            Self::Tikv { .. } => "tikv",
        }
    }

    pub(super) fn sqlite_path(&self) -> Option<&Path> {
        match self {
            Self::Sqlite { path } => Some(path),
            _ => None,
        }
    }

    fn redis_url(&self) -> Result<String> {
        let Self::Redis { url, url_env, .. } = self else {
            return Err(invalid("catalog is not configured for Redis"));
        };
        let value = match (url, url_env) {
            (Some(value), None) => value.clone(),
            (None, Some(name)) if safe_env_name(name) => std::env::var(name).map_err(|_| {
                ManagerError::new(
                    "unavailable",
                    "configured Redis credential environment is unavailable",
                )
            })?,
            _ => return Err(invalid("Redis requires exactly one local url or url_env")),
        };
        if value.len() > 8192 {
            return Err(invalid("Redis connection configuration exceeds its limit"));
        }
        let parsed = url::Url::parse(&value)
            .map_err(|_| invalid("invalid local Redis connection configuration"))?;
        if parsed.scheme() == "rediss" {
            return Err(unsupported("Redis TLS is not compiled into this build"));
        }
        if parsed.scheme() != "redis"
            || parsed.host_str().is_none()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(invalid("invalid local Redis connection configuration"));
        }
        redis::Client::open(value.as_str())
            .map_err(|_| invalid("invalid local Redis connection configuration"))?;
        Ok(value)
    }

    fn redis_identity(&self) -> Result<(String, u16, i64)> {
        let value = self.redis_url()?;
        let parsed = url::Url::parse(&value)
            .map_err(|_| invalid("invalid local Redis connection configuration"))?;
        let client = redis::Client::open(value.as_str())
            .map_err(|_| invalid("invalid local Redis connection configuration"))?;
        Ok((
            parsed.host_str().unwrap().to_ascii_lowercase(),
            parsed.port().unwrap_or(6379),
            client.get_connection_info().redis.db,
        ))
    }

    fn normalized_pd_endpoints(endpoints: &[String]) -> Result<Vec<String>> {
        if endpoints.is_empty() || endpoints.len() > 16 {
            return Err(invalid("TiKV requires between 1 and 16 PD endpoints"));
        }
        let mut normalized = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            if endpoint.len() > 2048 || endpoint.chars().any(char::is_whitespace) {
                return Err(invalid("invalid local TiKV endpoint configuration"));
            }
            let value = if endpoint.contains("://") {
                endpoint.clone()
            } else {
                format!("http://{endpoint}")
            };
            let parsed = url::Url::parse(&value)
                .map_err(|_| invalid("invalid local TiKV endpoint configuration"))?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
                || !matches!(parsed.path(), "" | "/")
            {
                return Err(invalid("invalid local TiKV endpoint configuration"));
            }
            normalized.push(parsed.to_string());
        }
        normalized.sort();
        normalized.dedup();
        Ok(normalized)
    }

    pub(super) fn validate(&self) -> Result<()> {
        match self {
            Self::Sqlite { path } => {
                if !path.is_absolute()
                    || path.file_name().is_none()
                    || path
                        .to_str()
                        .is_none_or(|value| value.contains(['?', '#', '%']))
                {
                    return Err(invalid(
                        "catalog references require absolute UTF-8 SQLite paths without URL escape characters",
                    ));
                }
                validate_catalog_file(path)?;
                path.parent().unwrap().canonicalize().map_err(storage)?;
            }
            Self::Redis {
                namespace,
                stable_cluster_id,
                url,
                url_env,
            } => {
                if !safe_identity(namespace, 128) || !safe_identity(stable_cluster_id, 128) {
                    return Err(invalid(
                        "remote catalog namespace and stable_cluster_id require safe nonempty identifiers",
                    ));
                }
                match (url, url_env) {
                    (Some(_), None) => {
                        self.redis_url()?;
                    }
                    (None, Some(name)) if safe_env_name(name) => {}
                    _ => return Err(invalid("Redis requires exactly one local url or url_env")),
                }
            }
            Self::Tikv {
                pd_endpoints,
                namespace,
                stable_cluster_id,
                tls,
            } => {
                if !safe_identity(namespace, 128) || !safe_identity(stable_cluster_id, 128) {
                    return Err(invalid(
                        "remote catalog namespace and stable_cluster_id require safe nonempty identifiers",
                    ));
                }
                let endpoints = Self::normalized_pd_endpoints(pd_endpoints)?;
                if tls.is_none()
                    && endpoints
                        .iter()
                        .any(|endpoint| endpoint.starts_with("https://"))
                {
                    return Err(invalid(
                        "HTTPS TiKV endpoints require explicit local TLS files",
                    ));
                }
                if let Some(tls) = tls {
                    for path in [&tls.ca_path, &tls.cert_path, &tls.key_path] {
                        if !path.is_absolute() {
                            return Err(invalid("TiKV TLS files must use absolute local paths"));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// SQLite intentionally hashes precisely the same serialized PathBuf as P1.
    pub(super) fn fingerprint(&self) -> Result<String> {
        match self {
            Self::Sqlite { path } => {
                let resolved = path
                    .parent()
                    .ok_or_else(|| invalid("invalid catalog path"))?
                    .canonicalize()
                    .map_err(storage)?
                    .join(
                        path.file_name()
                            .ok_or_else(|| invalid("invalid catalog path"))?,
                    );
                digest(&resolved)
            }
            Self::Redis {
                namespace,
                stable_cluster_id,
                ..
            } => {
                let (_, _, database) = self.redis_identity()?;
                digest(&(
                    "manager-catalog-v2",
                    "redis",
                    stable_cluster_id,
                    namespace,
                    database,
                ))
            }
            Self::Tikv {
                namespace,
                stable_cluster_id,
                ..
            } => digest(&("manager-catalog-v2", "tikv", stable_cluster_id, namespace)),
        }
    }

    /// Used to fence ambiguous initialization across a routing change. Auth is
    /// excluded; endpoint movement is distinct from permanent namespace identity.
    pub(super) fn transport_fingerprint(&self) -> Result<String> {
        match self {
            Self::Sqlite { .. } => self.fingerprint(),
            Self::Redis { .. } => digest(&self.redis_identity()?),
            Self::Tikv { pd_endpoints, .. } => {
                digest(&Self::normalized_pd_endpoints(pd_endpoints)?)
            }
        }
    }

    pub(super) async fn connect(
        &self,
        allow_create: bool,
        native: bool,
    ) -> Result<CatalogConnection> {
        self.validate()?;
        if native && !cfg!(feature = "native-packed-base") {
            return Err(unsupported(
                "native-packed-base is not compiled into this Manager",
            ));
        }
        let budget = V3MountBudget::defaults();
        match self {
            Self::Sqlite { path } => {
                if native {
                    return Err(unsupported("native-v2 requires Redis or TiKV"));
                }
                if !allow_create && !path.is_file() {
                    return Err(ManagerError::new(
                        "not_found",
                        "configured catalog file does not exist",
                    ));
                }
                let url = format!(
                    "sqlite://{}",
                    path.to_str()
                        .ok_or_else(|| invalid("catalog path must be UTF-8"))?
                );
                let store = Arc::new(
                    if allow_create {
                        SqliteWorkspaceStore::connect(&url).await
                    } else {
                        SqliteWorkspaceStore::connect_existing(&url).await
                    }
                    .map_err(catalog_error)?,
                );
                Ok(CatalogConnection {
                    store: store.clone(),
                    owner: StoreOwner::Sqlite(store),
                    raw: None,
                    guard: None,
                    namespace: None,
                    budget,
                    #[cfg(feature = "native-packed-base")]
                    native: None,
                    #[cfg(feature = "native-packed-base")]
                    native_raw: None,
                })
            }
            Self::Redis { namespace, .. } => {
                let url = self.redis_url()?;
                let backend = Arc::new(
                    RedisWorkspaceBackend::connect_read_only(&url, namespace)
                        .await
                        .map_err(catalog_error)?,
                );
                let store = Arc::new(
                    KvWorkspaceStore::from_arc(backend.clone())
                        .with_packed_reader_pin_budget(budget.clone()),
                );
                #[cfg(feature = "native-packed-base")]
                let native_raw: Option<
                    Arc<dyn crate::native_base::write::store::ControlStore>,
                > = if native {
                    Some(Arc::new(
                        crate::native_base::write::redis::RedisControlStore::connect(&url)
                            .await
                            .map_err(|_| unavailable_native())?,
                    ))
                } else {
                    None
                };
                Ok(CatalogConnection {
                    store,
                    owner: StoreOwner::Remote,
                    raw: Some(RawBackend::Redis(backend)),
                    guard: None,
                    namespace: Some(namespace.clone()),
                    budget,
                    #[cfg(feature = "native-packed-base")]
                    native: read_only_control(native_raw.as_ref()),
                    #[cfg(feature = "native-packed-base")]
                    native_raw,
                })
            }
            Self::Tikv {
                pd_endpoints,
                namespace,
                tls,
                ..
            } => {
                if native && tls.is_some() {
                    return Err(unsupported(
                        "native TiKV TLS control connections are not supported; plaintext fallback is forbidden",
                    ));
                }
                let backend = Arc::new(
                    match tls {
                        Some(tls) => {
                            let tls =
                                crate::workspace_overlay::stores::tikv::TiKvTlsConfig::from_paths(
                                    &tls.ca_path,
                                    &tls.cert_path,
                                    &tls.key_path,
                                )
                                .map_err(catalog_error)?;
                            TiKvWorkspaceBackend::connect_with_tls_and_budget(
                                pd_endpoints.clone(),
                                namespace,
                                budget.clone(),
                                tls,
                            )
                            .await
                        }
                        None => {
                            TiKvWorkspaceBackend::connect_with_budget(
                                pd_endpoints.clone(),
                                namespace,
                                budget.clone(),
                            )
                            .await
                        }
                    }
                    .map_err(catalog_error)?,
                );
                let store = Arc::new(
                    KvWorkspaceStore::from_arc(backend.clone())
                        .with_packed_reader_pin_budget(budget.clone()),
                );
                #[cfg(feature = "native-packed-base")]
                let native_raw: Option<
                    Arc<dyn crate::native_base::write::store::ControlStore>,
                > = if native {
                    match crate::native_base::write::tikv::TiKvControlStore::connect(pd_endpoints)
                        .await
                    {
                        Ok(control) => Some(Arc::new(control)),
                        Err(_) => {
                            let _ = backend.shutdown().await;
                            return Err(unavailable_native());
                        }
                    }
                } else {
                    None
                };
                Ok(CatalogConnection {
                    store,
                    owner: StoreOwner::Remote,
                    raw: Some(RawBackend::Tikv(backend)),
                    guard: None,
                    namespace: Some(namespace.clone()),
                    budget,
                    #[cfg(feature = "native-packed-base")]
                    native: read_only_control(native_raw.as_ref()),
                    #[cfg(feature = "native-packed-base")]
                    native_raw,
                })
            }
        }
    }
}

#[cfg(feature = "native-packed-base")]
fn unavailable_native() -> ManagerError {
    ManagerError::new("unavailable", "native control connection is unavailable")
}

enum RawBackend {
    Redis(Arc<RedisWorkspaceBackend>),
    Tikv(Arc<TiKvWorkspaceBackend>),
}
impl RawBackend {
    fn as_backend(&self) -> Arc<dyn WorkspaceKvBackend> {
        match self {
            Self::Redis(raw) => raw.clone(),
            Self::Tikv(raw) => raw.clone(),
        }
    }
    #[cfg(feature = "native-packed-base")]
    fn physical_guard_key(&self, namespace: &str) -> Vec<u8> {
        match self {
            Self::Redis(_) => {
                format!("{{brewfs-ws-v1}}:{namespace}:ws:v1/manager/controller").into_bytes()
            }
            Self::Tikv(_) => format!("{namespace}/ws:v1/manager/controller").into_bytes(),
        }
    }
}

enum StoreOwner {
    Sqlite(Arc<SqliteWorkspaceStore>),
    Remote,
    Guarded(Arc<KvWorkspaceStore<ManagerGuardedBackend>>),
}

pub(super) struct CatalogConnection {
    pub(super) store: Arc<dyn WorkspaceStore>,
    #[cfg(feature = "native-packed-base")]
    pub(super) native: Option<Arc<dyn crate::native_base::write::store::ControlStore>>,
    #[cfg(feature = "native-packed-base")]
    native_raw: Option<Arc<dyn crate::native_base::write::store::ControlStore>>,
    owner: StoreOwner,
    raw: Option<RawBackend>,
    guard: Option<Arc<ManagerGuard>>,
    namespace: Option<String>,
    budget: Arc<V3MountBudget>,
}

impl CatalogConnection {
    pub(super) fn raw_backend(&self) -> Option<Arc<dyn WorkspaceKvBackend>> {
        self.raw.as_ref().map(RawBackend::as_backend)
    }
    pub(super) fn guard(&self) -> Option<&Arc<ManagerGuard>> {
        self.guard.as_ref()
    }
    pub(super) fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }
    pub(super) fn backend_kind(&self) -> &'static str {
        match self.raw {
            Some(RawBackend::Redis(_)) => "redis",
            Some(RawBackend::Tikv(_)) => "tikv",
            None => "sqlite",
        }
    }

    pub(super) async fn claim(
        &mut self,
        journal: Uuid,
        session: Uuid,
        ttl_ns: u64,
        require_empty: bool,
        require_existing: bool,
    ) -> Result<()> {
        let Some(raw) = self.raw.as_ref() else {
            return Ok(());
        };
        if self.guard.is_some() {
            return Err(invalid("catalog handle already holds a Manager claim"));
        }
        if require_empty && require_existing {
            return Err(invalid(
                "catalog ownership cannot require both an empty and existing claim",
            ));
        }
        let guard = if require_existing {
            ManagerGuard::acquire_existing(raw.as_backend(), journal, session, ttl_ns).await
        } else {
            ManagerGuard::acquire(raw.as_backend(), journal, session, ttl_ns, require_empty).await
        }
        .map_err(|error| match error {
            crate::workspace_overlay::error::WorkspaceError::Busy => ManagerError::new(
                "session_busy",
                "previous Manager session still holds the catalog; retry the original operation after lease expiry",
            ),
            other => catalog_error(other),
        })?;
        let store = Arc::new(
            KvWorkspaceStore::new(guard.guarded_backend())
                .with_packed_reader_pin_budget(self.budget.clone()),
        );
        self.store = store.clone();
        self.owner = StoreOwner::Guarded(store);
        #[cfg(feature = "native-packed-base")]
        if let Some(control) = &self.native_raw {
            self.native = Some(Arc::new(GuardedControlStore {
                inner: control.clone(),
                guard: guard.clone(),
                physical_guard: raw.physical_guard_key(self.namespace.as_deref().unwrap()),
            }));
        }
        self.guard = Some(guard);
        Ok(())
    }

    pub(super) async fn initialize_empty(
        &self,
        request: CreateVolumeRoot,
    ) -> Result<WorkspaceRecord> {
        match &self.owner {
            StoreOwner::Sqlite(store) => store.create_volume_root_in_empty_namespace(request).await,
            StoreOwner::Guarded(store) => {
                store.create_volume_root_in_empty_namespace(request).await
            }
            StoreOwner::Remote => {
                return Err(invalid(
                    "remote initialization requires an acquired Manager claim",
                ));
            }
        }
        .map_err(catalog_error)
    }

    pub(super) async fn validate_schema(&self) -> Result<()> {
        if let Some(RawBackend::Redis(raw)) = &self.raw {
            raw.validate_manager_catalog_index()
                .await
                .map_err(catalog_error)?;
        }
        match &self.owner {
            StoreOwner::Sqlite(store) => store
                .validate_manager_catalog_schema()
                .await
                .map_err(catalog_error),
            _ => {
                self.store
                    .load_volume_header()
                    .await
                    .map_err(catalog_error)?;
                Ok(())
            }
        }
    }

    pub(super) async fn native_locator(&self) -> Result<Option<Vec<u8>>> {
        match &self.raw {
            Some(RawBackend::Redis(raw)) => raw.load_manager_native_locator().await,
            Some(RawBackend::Tikv(raw)) => raw.load_manager_native_locator().await,
            None => return Ok(None),
        }
        .map_err(catalog_error)
    }

    pub(super) async fn has_packed_bindings(&self) -> Result<bool> {
        if let StoreOwner::Sqlite(store) = &self.owner {
            return store
                .has_packed_catalog_bindings()
                .await
                .map_err(catalog_error);
        }
        if let Some(RawBackend::Redis(raw)) = &self.raw {
            for prefix in [b"packed/v3/history/".as_slice(), b"packed/v3/current/"] {
                if raw.has_raw_prefix(prefix).await.map_err(catalog_error)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        let backend = self
            .raw_backend()
            .ok_or_else(|| invalid("remote catalog handle is missing"))?;
        let limits = KvReadLimits {
            max_records: 1,
            max_key_bytes: 1024,
            max_value_bytes: 48 << 10,
            max_total_bytes: 49 << 10,
            max_response_bytes: 64 << 10,
            max_data_requests: 1,
        };
        for prefix in [b"packed/v3/history/".as_slice(), b"packed/v3/current/"] {
            if !backend
                .scan_prefix_page_with_byte_limits(prefix, None, limits)
                .await
                .map_err(catalog_error)?
                .is_empty()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) async fn shutdown(&self) -> Result<()> {
        self.store
            .shutdown_metadata_backend()
            .await
            .map_err(catalog_error)
    }
}

impl Drop for CatalogConnection {
    fn drop(&mut self) {
        // The journal owns lease release. A connection only drains its client,
        // including read-only handles abandoned when admission fails.
        if let (Some(raw), Ok(runtime)) =
            (self.raw_backend(), tokio::runtime::Handle::try_current())
        {
            runtime.spawn(async move {
                let _ = raw.shutdown_metadata_backend().await;
            });
        }
    }
}

#[cfg(feature = "native-packed-base")]
struct GuardedControlStore {
    inner: Arc<dyn crate::native_base::write::store::ControlStore>,
    guard: Arc<ManagerGuard>,
    physical_guard: Vec<u8>,
}

#[cfg(feature = "native-packed-base")]
struct ReadOnlyControlStore {
    inner: Arc<dyn crate::native_base::write::store::ControlStore>,
}

#[cfg(feature = "native-packed-base")]
fn read_only_control(
    inner: Option<&Arc<dyn crate::native_base::write::store::ControlStore>>,
) -> Option<Arc<dyn crate::native_base::write::store::ControlStore>> {
    inner.map(|inner| {
        Arc::new(ReadOnlyControlStore {
            inner: inner.clone(),
        }) as Arc<dyn crate::native_base::write::store::ControlStore>
    })
}

#[cfg(feature = "native-packed-base")]
#[async_trait::async_trait]
impl crate::native_base::write::store::ControlStore for ReadOnlyControlStore {
    async fn get(
        &self,
        key: &[u8],
    ) -> std::result::Result<Option<Vec<u8>>, crate::native_base::write::store::StoreError> {
        self.inner.get(key).await
    }
    async fn run(
        &self,
        txn: crate::native_base::write::store::Txn,
    ) -> std::result::Result<(), crate::native_base::write::store::StoreError> {
        if txn.writes.is_empty() {
            return self.inner.run(txn).await;
        }
        Err(crate::native_base::write::store::StoreError::Backend(
            "Manager ownership has not been acquired".into(),
        ))
    }
    async fn scan(
        &self,
        prefix: &[u8],
    ) -> std::result::Result<Vec<(Vec<u8>, Vec<u8>)>, crate::native_base::write::store::StoreError>
    {
        self.inner.scan(prefix).await
    }
}

#[cfg(feature = "native-packed-base")]
#[async_trait::async_trait]
impl crate::native_base::write::store::ControlStore for GuardedControlStore {
    async fn get(
        &self,
        key: &[u8],
    ) -> std::result::Result<Option<Vec<u8>>, crate::native_base::write::store::StoreError> {
        self.inner.get(key).await
    }
    async fn run(
        &self,
        mut txn: crate::native_base::write::store::Txn,
    ) -> std::result::Result<(), crate::native_base::write::store::StoreError> {
        use crate::native_base::write::store::StoreError;
        let _mutation = self.guard.lock_mutations().await;
        let fence = self.guard.fence().map_err(|_| StoreError::Conflict)?;
        let expected = fence.check.expected.ok_or(StoreError::Conflict)?;
        if txn
            .writes
            .iter()
            .any(|(key, _)| key == &self.physical_guard)
        {
            return Err(StoreError::Conflict);
        }
        txn = txn.check_bytes(self.physical_guard.clone(), expected);
        self.inner.run_before(txn, fence.expires_at_ns).await
    }
    async fn scan(
        &self,
        prefix: &[u8],
    ) -> std::result::Result<Vec<(Vec<u8>, Vec<u8>)>, crate::native_base::write::store::StoreError>
    {
        self.inner.scan(prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn redis_config(url: &str, namespace: &str, cluster: &str) -> CatalogConfig {
        serde_json::from_value(serde_json::json!({"backend":"redis","url":url,"namespace":namespace,"stable_cluster_id":cluster})).unwrap()
    }

    #[test]
    fn legacy_sqlite_configuration_and_fingerprint_are_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.sqlite");
        let config: CatalogConfig =
            serde_json::from_value(serde_json::json!({"backend":"sqlite","path":path})).unwrap();
        config.validate().unwrap();
        assert_eq!(
            config.fingerprint().unwrap(),
            digest(
                &directory
                    .path()
                    .canonicalize()
                    .unwrap()
                    .join("catalog.sqlite")
            )
            .unwrap()
        );
    }

    #[test]
    fn redis_credentials_and_routing_are_distinct_from_immutable_scope() {
        let original = redis_config(
            "redis://user:secret-a@localhost:6379/2",
            "campaign",
            "cluster-a",
        );
        let rotated = redis_config(
            "redis://other:secret-b@localhost:6379/2",
            "campaign",
            "cluster-a",
        );
        let moved = redis_config(
            "redis://other:secret-b@replacement:6380/2",
            "campaign",
            "cluster-a",
        );
        assert_eq!(
            original.fingerprint().unwrap(),
            rotated.fingerprint().unwrap()
        );
        assert_eq!(
            original.transport_fingerprint().unwrap(),
            rotated.transport_fingerprint().unwrap()
        );
        assert_eq!(
            original.fingerprint().unwrap(),
            moved.fingerprint().unwrap()
        );
        assert_ne!(
            original.transport_fingerprint().unwrap(),
            moved.transport_fingerprint().unwrap()
        );
        for rebound in [
            redis_config("redis://localhost/3", "campaign", "cluster-a"),
            redis_config("redis://localhost/2", "other", "cluster-a"),
            redis_config("redis://localhost/2", "campaign", "cluster-b"),
        ] {
            assert_ne!(
                original.fingerprint().unwrap(),
                rebound.fingerprint().unwrap()
            );
        }
        assert!(!format!("{original:?}").contains("secret"));
        assert!(!format!("{original:?}").contains("localhost"));
    }

    #[test]
    fn invalid_remote_configuration_never_echoes_credentials() {
        let config = redis_config(
            "redis://user:do-not-report@localhost/2?unsupported=1",
            "campaign",
            "cluster-a",
        );
        let message = config.validate().unwrap_err().to_string();
        assert!(!message.contains("do-not-report"));
        assert!(!message.contains("localhost"));
        assert!(serde_json::from_value::<CatalogConfig>(serde_json::json!({"backend":"redis","url":"redis://localhost","namespace":"campaign","stable_cluster_id":"cluster","path":"/arbitrary"})).is_err());
    }

    #[test]
    fn missing_credential_environment_does_not_prevent_structural_admission() {
        let variable = format!("BREWFS_MANAGER_MISSING_{}", Uuid::new_v4().simple());
        let config: CatalogConfig = serde_json::from_value(serde_json::json!({
            "backend":"redis", "url_env":variable,
            "namespace":"campaign", "stable_cluster_id":"cluster-a"
        }))
        .unwrap();
        config.validate().unwrap();
        let error = config.fingerprint().unwrap_err();
        assert_eq!(error.code, "unavailable");
        assert!(!error.message.contains(&variable));
    }

    #[test]
    fn tikv_endpoint_reordering_preserves_transport_but_rebinding_changes_scope() {
        let make = |endpoints: Vec<&str>, namespace: &str| {
            serde_json::from_value::<CatalogConfig>(serde_json::json!({
                "backend":"tikv", "pd_endpoints":endpoints,
                "namespace":namespace, "stable_cluster_id":"cluster-a"
            }))
            .unwrap()
        };
        let first = make(vec!["pd-a:2379", "pd-b:2379"], "campaign");
        let reorder = make(vec!["pd-b:2379", "pd-a:2379"], "campaign");
        let routed = make(vec!["pd-c:2379"], "campaign");
        assert_eq!(first.fingerprint().unwrap(), routed.fingerprint().unwrap());
        assert_eq!(
            first.transport_fingerprint().unwrap(),
            reorder.transport_fingerprint().unwrap()
        );
        assert_ne!(
            first.transport_fingerprint().unwrap(),
            routed.transport_fingerprint().unwrap()
        );
        assert_ne!(
            first.fingerprint().unwrap(),
            make(vec!["pd-a:2379"], "other").fingerprint().unwrap()
        );
        assert!(
            make(vec!["https://pd-a:2379"], "campaign")
                .validate()
                .is_err()
        );
    }

    #[test]
    fn redis_connection_configuration_does_not_accept_cluster_or_sentinel_discovery() {
        for url in [
            "redis+cluster://private-host/0",
            "redis+sentinel://private-host/0",
            "rediss://private-host/0",
        ] {
            let error = redis_config(url, "catalog", "deployment")
                .validate()
                .unwrap_err();
            assert!(!error.message.contains("private-host"));
        }
        for field in ["cluster_nodes", "sentinel_service_name", "sentinels"] {
            let mut config = serde_json::json!({"backend":"redis", "url":"redis://private-host/0", "namespace":"catalog", "stable_cluster_id":"deployment"});
            config[field] = "secret-marker".into();
            assert!(serde_json::from_value::<CatalogConfig>(config).is_err());
        }
    }

    #[cfg(feature = "native-packed-base")]
    #[tokio::test]
    async fn native_tikv_tls_is_rejected_before_network_or_credential_file_access() {
        let dir = tempfile::tempdir().unwrap();
        let config: CatalogConfig = serde_json::from_value(serde_json::json!({
            "backend":"tikv", "pd_endpoints":["https://private-host.invalid:2379"],
            "namespace":"catalog", "stable_cluster_id":"deployment",
            "tls":{"ca_path":dir.path().join("missing-ca"), "cert_path":dir.path().join("missing-cert"), "key_path":dir.path().join("private-key-marker")}
        })).unwrap();
        config.validate().unwrap();
        let error = match config.connect(false, true).await {
            Ok(_) => panic!("native TiKV TLS must not fall back to a plaintext connection"),
            Err(error) => error,
        };
        assert_eq!(error.code, "unsupported");
        assert!(!error.message.contains("private-host"));
        assert!(!error.message.contains("private-key-marker"));
    }

    #[cfg(feature = "native-packed-base")]
    #[tokio::test]
    async fn unclaimed_native_connection_and_unsupported_deadline_cannot_write() {
        use crate::native_base::write::{
            memory::MemoryControlStore,
            store::{ControlStore, Txn},
        };
        let raw: Arc<dyn ControlStore> = Arc::new(MemoryControlStore::new());
        let reader = read_only_control(Some(&raw)).unwrap();
        reader
            .run(Txn::new().check_absent(b"target".to_vec()))
            .await
            .unwrap();
        raw.run(Txn::new().put(b"source".to_vec(), vec![7]))
            .await
            .unwrap();
        reader
            .run(Txn::new().check_bytes(b"source".to_vec(), vec![7]))
            .await
            .unwrap();
        assert!(
            reader
                .run(Txn::new().check_bytes(b"source".to_vec(), vec![8]))
                .await
                .is_err()
        );
        assert!(
            reader
                .run(Txn::new().put(b"target".to_vec(), vec![1]))
                .await
                .is_err()
        );
        assert_eq!(raw.get(b"target").await.unwrap(), None);
        assert!(
            raw.run_before(Txn::new().put(b"target".to_vec(), vec![2]), i64::MAX)
                .await
                .is_err()
        );
        assert_eq!(raw.get(b"target").await.unwrap(), None);
    }

    #[cfg(feature = "native-packed-base")]
    #[tokio::test]
    async fn native_mutation_waits_for_a_committed_renewal_reply() {
        use crate::native_base::write::store::{ControlStore, Expect, StoreError, Txn};
        use crate::workspace_overlay::stores::{
            kv_backend::{KvCheck, KvWrite},
            manager_guard::{
                MANAGER_CONTROLLER_KEY,
                tests::{ClockStore, fixture},
            },
        };
        use std::sync::atomic::Ordering;

        // Both interfaces observe the same atomic controller row, as the real
        // native and catalog connections do in Redis and TiKV.
        struct NativeClock(Arc<ClockStore>);
        #[async_trait::async_trait]
        impl ControlStore for NativeClock {
            async fn get(&self, key: &[u8]) -> std::result::Result<Option<Vec<u8>>, StoreError> {
                self.0.get(key).await.map_err(|_| StoreError::Conflict)
            }
            async fn run(&self, txn: Txn) -> std::result::Result<(), StoreError> {
                self.run_before(txn, i64::MAX).await
            }
            async fn run_before(
                &self,
                txn: Txn,
                before: i64,
            ) -> std::result::Result<(), StoreError> {
                let checks = txn
                    .checks
                    .into_iter()
                    .map(|(key, expect)| KvCheck {
                        key,
                        expected: match expect {
                            Expect::Absent => None,
                            Expect::Bytes(value) => Some(value),
                        },
                    })
                    .collect::<Vec<_>>();
                let writes = txn
                    .writes
                    .into_iter()
                    .map(|(key, value)| match value {
                        Some(value) => KvWrite::Put { key, value },
                        None => KvWrite::Delete { key },
                    })
                    .collect::<Vec<_>>();
                match self
                    .0
                    .compare_and_swap_before(&checks, &writes, before)
                    .await
                {
                    Ok(true) => Ok(()),
                    _ => Err(StoreError::Conflict),
                }
            }
            async fn scan(
                &self,
                prefix: &[u8],
            ) -> std::result::Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError> {
                self.0
                    .scan_prefix(prefix)
                    .await
                    .map(|rows| rows.into_iter().map(|row| (row.key, row.value)).collect())
                    .map_err(|_| StoreError::Conflict)
            }
        }

        let raw = fixture();
        let guard = ManagerGuard::acquire(raw.clone(), Uuid::new_v4(), Uuid::new_v4(), 100, true)
            .await
            .unwrap();
        let control = GuardedControlStore {
            inner: Arc::new(NativeClock(raw.clone())),
            guard: guard.clone(),
            physical_guard: MANAGER_CONTROLLER_KEY.to_vec(),
        };
        raw.now.store(1_050, Ordering::SeqCst);
        raw.pause_reply.store(true, Ordering::SeqCst);
        let renewing = tokio::spawn(async move { guard.renew().await });
        raw.reply_committed.notified().await;
        let mut mutation = tokio::spawn(async move {
            control
                .run(
                    Txn::new()
                        .check_absent(b"native-effect".to_vec())
                        .put(b"native-effect".to_vec(), vec![7]),
                )
                .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut mutation)
                .await
                .is_err(),
            "native publication must wait for the committed renewal reply"
        );
        raw.resume_reply.notify_one();
        renewing.await.unwrap().unwrap();
        mutation.await.unwrap().unwrap();
        assert_eq!(raw.get(b"native-effect").await.unwrap(), Some(vec![7]));
    }
}
