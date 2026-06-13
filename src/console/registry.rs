use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct VolumeRegistry {
    path: Arc<PathBuf>,
    lock: Arc<Mutex<()>>,
}

impl VolumeRegistry {
    pub fn new(state_dir: PathBuf) -> Self {
        Self {
            path: Arc::new(state_dir.join("volumes.json")),
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn list(&self) -> Result<Vec<VolumeResponse>, RegistryError> {
        let _guard = self.lock.lock().await;
        let file = self.load_unlocked().await?;
        Ok(file.volumes.iter().map(StoredVolume::to_response).collect())
    }

    pub async fn create(
        &self,
        request: CreateVolumeRequest,
    ) -> Result<VolumeResponse, RegistryError> {
        request.validate()?;
        let _guard = self.lock.lock().await;
        let mut file = self.load_unlocked().await?;
        let now = Utc::now();
        let volume = StoredVolume {
            id: Uuid::now_v7().to_string(),
            name: request.name.trim().to_owned(),
            description: request.description.and_then(non_empty_trimmed),
            labels: request.labels,
            created_at: now,
            updated_at: now,
            mount_config: StoredVolumeMountConfig {
                mount_point: request.mount_config.mount_point.and_then(non_empty_trimmed),
                data_backend: request.mount_config.data_backend.trim().to_owned(),
                data_dir: request.mount_config.data_dir.and_then(non_empty_trimmed),
                meta_backend: request.mount_config.meta_backend.trim().to_owned(),
                meta_url: request.mount_config.meta_url.and_then(non_empty_trimmed),
                chunk_size: request.mount_config.chunk_size,
                block_size: request.mount_config.block_size,
            },
        };
        let response = volume.to_response();
        file.volumes.push(volume);
        self.store_unlocked(&file).await?;
        Ok(response)
    }

    pub async fn get(&self, volume_id: &str) -> Result<VolumeResponse, RegistryError> {
        let _guard = self.lock.lock().await;
        let file = self.load_unlocked().await?;
        file.volumes
            .iter()
            .find(|volume| volume.id == volume_id)
            .map(StoredVolume::to_response)
            .ok_or_else(|| RegistryError::not_found(format!("volume not found: {volume_id}")))
    }

    pub async fn update(
        &self,
        volume_id: &str,
        request: UpdateVolumeRequest,
    ) -> Result<VolumeResponse, RegistryError> {
        request.validate()?;
        let _guard = self.lock.lock().await;
        let mut file = self.load_unlocked().await?;
        let response = {
            let volume = file
                .volumes
                .iter_mut()
                .find(|volume| volume.id == volume_id)
                .ok_or_else(|| {
                    RegistryError::not_found(format!("volume not found: {volume_id}"))
                })?;

            if let Some(name) = request.name {
                volume.name = name.trim().to_owned();
            }
            if let Some(description) = request.description {
                volume.description = description.and_then(non_empty_trimmed);
            }
            if let Some(labels) = request.labels {
                volume.labels = labels;
            }
            volume.updated_at = Utc::now();
            volume.to_response()
        };
        self.store_unlocked(&file).await?;
        Ok(response)
    }

    pub async fn delete(&self, volume_id: &str) -> Result<(), RegistryError> {
        let _guard = self.lock.lock().await;
        let mut file = self.load_unlocked().await?;
        let original_len = file.volumes.len();
        file.volumes.retain(|volume| volume.id != volume_id);
        if file.volumes.len() == original_len {
            return Err(RegistryError::not_found(format!(
                "volume not found: {volume_id}"
            )));
        }
        self.store_unlocked(&file).await
    }

    async fn load_unlocked(&self) -> Result<VolumeRegistryFile, RegistryError> {
        match tokio::fs::read(self.path.as_ref()).await {
            Ok(data) => serde_json::from_slice(&data)
                .map_err(|err| RegistryError::internal(format!("invalid registry JSON: {err}"))),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Ok(VolumeRegistryFile::default())
            }
            Err(err) => Err(RegistryError::internal(format!(
                "failed to read volume registry {}: {err}",
                self.path.display()
            ))),
        }
    }

    async fn store_unlocked(&self, file: &VolumeRegistryFile) -> Result<(), RegistryError> {
        let Some(parent) = self.path.parent() else {
            return Err(RegistryError::internal(
                "volume registry path has no parent",
            ));
        };
        tokio::fs::create_dir_all(parent).await.map_err(|err| {
            RegistryError::internal(format!(
                "failed to create volume registry directory {}: {err}",
                parent.display()
            ))
        })?;
        let data = serde_json::to_vec_pretty(file).map_err(|err| {
            RegistryError::internal(format!("failed to encode volume registry: {err}"))
        })?;
        let tmp_path = self.path.with_extension("json.tmp");
        tokio::fs::write(&tmp_path, data).await.map_err(|err| {
            RegistryError::internal(format!(
                "failed to write temporary volume registry {}: {err}",
                tmp_path.display()
            ))
        })?;
        tokio::fs::rename(&tmp_path, self.path.as_ref())
            .await
            .map_err(|err| {
                RegistryError::internal(format!(
                    "failed to replace volume registry {}: {err}",
                    self.path.display()
                ))
            })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateVolumeRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub mount_config: CreateVolumeMountConfig,
}

impl CreateVolumeRequest {
    fn validate(&self) -> Result<(), RegistryError> {
        if self.name.trim().is_empty() {
            return Err(RegistryError::invalid_config(
                "volume name must not be empty",
            ));
        }
        if self.mount_config.data_backend.trim().is_empty() {
            return Err(RegistryError::invalid_config(
                "data backend must not be empty",
            ));
        }
        if self.mount_config.meta_backend.trim().is_empty() {
            return Err(RegistryError::invalid_config(
                "meta backend must not be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateVolumeMountConfig {
    #[serde(default)]
    pub mount_point: Option<String>,
    pub data_backend: String,
    #[serde(default)]
    pub data_dir: Option<String>,
    pub meta_backend: String,
    #[serde(default)]
    pub meta_url: Option<String>,
    #[serde(default)]
    pub chunk_size: Option<u64>,
    #[serde(default)]
    pub block_size: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateVolumeRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<Option<String>>,
    #[serde(default)]
    pub labels: Option<BTreeMap<String, String>>,
}

impl UpdateVolumeRequest {
    fn validate(&self) -> Result<(), RegistryError> {
        if self
            .name
            .as_deref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(RegistryError::invalid_config(
                "volume name must not be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VolumeResponse {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub mount_config: VolumeMountConfigResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VolumeMountConfigResponse {
    pub mount_point: Option<String>,
    pub data_backend: String,
    pub data_dir: Option<String>,
    pub meta_backend: String,
    pub meta_url_redacted: Option<String>,
    pub chunk_size: Option<u64>,
    pub block_size: Option<u64>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct VolumeRegistryFile {
    #[serde(default)]
    volumes: Vec<StoredVolume>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StoredVolume {
    id: String,
    name: String,
    description: Option<String>,
    labels: BTreeMap<String, String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    mount_config: StoredVolumeMountConfig,
}

impl StoredVolume {
    fn to_response(&self) -> VolumeResponse {
        VolumeResponse {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            labels: self.labels.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            mount_config: VolumeMountConfigResponse {
                mount_point: self.mount_config.mount_point.clone(),
                data_backend: self.mount_config.data_backend.clone(),
                data_dir: self.mount_config.data_dir.clone(),
                meta_backend: self.mount_config.meta_backend.clone(),
                meta_url_redacted: self
                    .mount_config
                    .meta_url
                    .as_deref()
                    .map(redact_connection_string),
                chunk_size: self.mount_config.chunk_size,
                block_size: self.mount_config.block_size,
            },
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StoredVolumeMountConfig {
    mount_point: Option<String>,
    data_backend: String,
    data_dir: Option<String>,
    meta_backend: String,
    meta_url: Option<String>,
    chunk_size: Option<u64>,
    block_size: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct RegistryError {
    code: &'static str,
    message: String,
}

impl RegistryError {
    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn invalid_config(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_config",
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            code: "registry_error",
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: "not_found",
            message: message.into(),
        }
    }
}

fn non_empty_trimmed(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn redact_connection_string(value: &str) -> String {
    let value = redact_authority_password(value);
    redact_sensitive_query_params(&value)
}

fn redact_authority_password(value: &str) -> String {
    let Some(scheme_index) = value.find("://") else {
        return value.to_owned();
    };
    let authority_start = scheme_index + 3;
    let Some(at_offset) = value[authority_start..].find('@') else {
        return value.to_owned();
    };
    let authority_end = authority_start + at_offset;
    let Some(password_colon_offset) = value[authority_start..authority_end].rfind(':') else {
        return value.to_owned();
    };
    let password_start = authority_start + password_colon_offset + 1;
    let mut redacted = String::with_capacity(value.len() + 8);
    redacted.push_str(&value[..password_start]);
    redacted.push_str("<redacted>");
    redacted.push_str(&value[authority_end..]);
    redacted
}

fn redact_sensitive_query_params(value: &str) -> String {
    let Some(query_marker) = value.find('?') else {
        return value.to_owned();
    };
    let query_start = query_marker + 1;
    let fragment_offset = value[query_start..]
        .find('#')
        .map(|offset| query_start + offset)
        .unwrap_or(value.len());
    let query = &value[query_start..fragment_offset];
    let redacted_query = query
        .split('&')
        .map(redact_query_pair)
        .collect::<Vec<_>>()
        .join("&");

    let mut redacted = String::with_capacity(value.len());
    redacted.push_str(&value[..query_start]);
    redacted.push_str(&redacted_query);
    redacted.push_str(&value[fragment_offset..]);
    redacted
}

fn redact_query_pair(pair: &str) -> String {
    let Some(eq_index) = pair.find('=') else {
        return pair.to_owned();
    };
    let key = &pair[..eq_index];
    if is_sensitive_query_key(key) {
        format!("{key}=<redacted>")
    } else {
        pair.to_owned()
    }
}

fn is_sensitive_query_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    normalized.contains("secret")
        || normalized.contains("password")
        || normalized.contains("passwd")
        || normalized.contains("token")
        || normalized.contains("access_key")
        || normalized.contains("access-key")
        || normalized.contains("accesskey")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn create_request() -> CreateVolumeRequest {
        CreateVolumeRequest {
            name: "dev-local".into(),
            description: Some("local development".into()),
            labels: BTreeMap::from([("env".into(), "dev".into())]),
            mount_config: CreateVolumeMountConfig {
                mount_point: Some("/mnt/brewfs".into()),
                data_backend: "local-fs".into(),
                data_dir: Some("/var/lib/brewfs/data".into()),
                meta_backend: "sqlx".into(),
                meta_url: Some("postgres://brewfs:secret@db.example/brewfs".into()),
                chunk_size: Some(67_108_864),
                block_size: Some(4_194_304),
            },
        }
    }

    #[tokio::test]
    async fn create_persists_volume_and_redacts_secret_in_response() {
        let dir = tempdir().unwrap();
        let registry = VolumeRegistry::new(dir.path().to_path_buf());

        let volume = registry.create(create_request()).await.unwrap();

        assert_eq!(volume.name, "dev-local");
        assert_eq!(
            volume.mount_config.meta_url_redacted.as_deref(),
            Some("postgres://brewfs:<redacted>@db.example/brewfs")
        );
        let response_json = serde_json::to_string(&volume).unwrap();
        assert!(!response_json.contains("secret"));

        let registry = VolumeRegistry::new(dir.path().to_path_buf());
        let volumes = registry.list().await.unwrap();
        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].id, volume.id);
    }

    #[tokio::test]
    async fn create_redacts_sensitive_query_values_in_response() {
        let dir = tempdir().unwrap();
        let registry = VolumeRegistry::new(dir.path().to_path_buf());
        let mut request = create_request();
        request.mount_config.meta_url = Some(
            "s3://bucket/path?access_key_id=AKIA_TEST&secret_access_key=SECRET_TEST&session_token=TOKEN_TEST&endpoint=http://127.0.0.1:9000"
                .into(),
        );

        let volume = registry.create(request).await.unwrap();

        assert_eq!(
            volume.mount_config.meta_url_redacted.as_deref(),
            Some(
                "s3://bucket/path?access_key_id=<redacted>&secret_access_key=<redacted>&session_token=<redacted>&endpoint=http://127.0.0.1:9000"
            )
        );
        let response_json = serde_json::to_string(&volume).unwrap();
        assert!(!response_json.contains("AKIA_TEST"));
        assert!(!response_json.contains("SECRET_TEST"));
        assert!(!response_json.contains("TOKEN_TEST"));
    }

    #[tokio::test]
    async fn create_rejects_empty_names() {
        let dir = tempdir().unwrap();
        let registry = VolumeRegistry::new(dir.path().to_path_buf());
        let mut request = create_request();
        request.name = "  ".into();

        let err = registry.create(request).await.unwrap_err();

        assert_eq!(err.code(), "invalid_config");
    }

    #[tokio::test]
    async fn update_and_delete_manage_existing_volume_metadata() {
        let dir = tempdir().unwrap();
        let registry = VolumeRegistry::new(dir.path().to_path_buf());
        let created = registry.create(create_request()).await.unwrap();

        let updated = registry
            .update(
                &created.id,
                UpdateVolumeRequest {
                    name: Some("prod-local".into()),
                    description: Some(None),
                    labels: Some(BTreeMap::from([("env".into(), "prod".into())])),
                },
            )
            .await
            .unwrap();

        assert_eq!(updated.id, created.id);
        assert_eq!(updated.name, "prod-local");
        assert_eq!(updated.description, None);
        assert_eq!(updated.labels.get("env").map(String::as_str), Some("prod"));
        assert!(updated.updated_at >= created.updated_at);

        let fetched = registry.get(&created.id).await.unwrap();
        assert_eq!(fetched.name, "prod-local");

        registry.delete(&created.id).await.unwrap();
        let err = registry.get(&created.id).await.unwrap_err();
        assert_eq!(err.code(), "not_found");
    }
}
