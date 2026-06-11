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
    async fn create_rejects_empty_names() {
        let dir = tempdir().unwrap();
        let registry = VolumeRegistry::new(dir.path().to_path_buf());
        let mut request = create_request();
        request.name = "  ".into();

        let err = registry.create(request).await.unwrap_err();

        assert_eq!(err.code(), "invalid_config");
    }
}
