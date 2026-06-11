use super::{
    AuthMode, ConsoleState,
    registry::{CreateVolumeRequest, RegistryError, VolumeResponse},
};
use crate::{
    control::{
        client::send_request,
        protocol::{ControlRequest, ControlResponse},
        runtime::InstanceRecord,
    },
    meta::store::MetaStoreCapabilities,
};
use axum::{
    Json,
    extract::{Path, State},
};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct HealthResponse {
    pub service: &'static str,
    pub version: &'static str,
    pub commit_short: &'static str,
    pub auth_mode: AuthMode,
    pub integrations: HealthIntegrations,
    pub static_assets_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct HealthIntegrations {
    pub csi_dashboard: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ListVolumesResponse {
    pub volumes: Vec<VolumeResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ListInstancesResponse {
    pub instances: Vec<InstanceResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct InstanceResponse {
    pub pid: u32,
    pub mount_point: String,
    pub socket_path: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct InstanceInfoResponse {
    pub pid: u32,
    pub mount_point: String,
    pub started_at: i64,
    pub version: String,
    pub meta_backend: String,
    pub capabilities: MetaStoreCapabilities,
}

impl From<InstanceRecord> for InstanceResponse {
    fn from(record: InstanceRecord) -> Self {
        Self {
            pid: record.pid,
            mount_point: record.mount_point,
            socket_path: record.socket_path.to_string_lossy().into_owned(),
            started_at: record.started_at,
        }
    }
}

impl HealthResponse {
    pub fn from_state(state: &ConsoleState, static_assets_available: bool) -> Self {
        Self {
            service: "brewfs-console",
            version: env!("CARGO_PKG_VERSION"),
            commit_short: env!("BREWFS_GIT_COMMIT_SHORT"),
            auth_mode: state.auth.mode(),
            integrations: HealthIntegrations {
                csi_dashboard: state.csi_dashboard,
            },
            static_assets_available,
        }
    }
}

pub async fn health(State(state): State<ConsoleState>) -> Json<HealthResponse> {
    let static_assets_available = state.static_dir.join("index.html").is_file();
    Json(HealthResponse::from_state(&state, static_assets_available))
}

pub async fn list_volumes(
    State(state): State<ConsoleState>,
) -> Result<Json<ListVolumesResponse>, ApiErrorResponse> {
    let volumes = state
        .registry
        .list()
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(Json(ListVolumesResponse { volumes }))
}

pub async fn create_volume(
    State(state): State<ConsoleState>,
    Json(request): Json<CreateVolumeRequest>,
) -> Result<(StatusCode, Json<VolumeResponse>), ApiErrorResponse> {
    let volume = state
        .registry
        .create(request)
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok((StatusCode::CREATED, Json(volume)))
}

pub async fn list_instances(
    State(state): State<ConsoleState>,
) -> Result<Json<ListInstancesResponse>, ApiErrorResponse> {
    let instances = state
        .runtime_registry
        .list_instances()
        .await
        .map_err(|err| {
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "runtime_registry_error",
                format!("failed to read runtime registry: {err}"),
            )
        })?
        .into_iter()
        .map(InstanceResponse::from)
        .collect();
    Ok(Json(ListInstancesResponse { instances }))
}

pub async fn get_instance_info(
    State(state): State<ConsoleState>,
    Path(pid): Path<u32>,
) -> Result<Json<InstanceInfoResponse>, ApiErrorResponse> {
    let record = state
        .runtime_registry
        .list_instances()
        .await
        .map_err(|err| {
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "runtime_registry_error",
                format!("failed to read runtime registry: {err}"),
            )
        })?
        .into_iter()
        .find(|record| record.pid == pid)
        .ok_or_else(|| {
            json_error(
                StatusCode::NOT_FOUND,
                "not_found",
                "runtime instance not found",
            )
        })?;

    let response = tokio::time::timeout(
        Duration::from_secs(2),
        send_request(&record.socket_path, &ControlRequest::GetInfo),
    )
    .await
    .map_err(|_| {
        json_error(
            StatusCode::BAD_GATEWAY,
            "control_plane_error",
            "control-plane request timed out",
        )
    })?
    .map_err(|err| {
        json_error(
            StatusCode::BAD_GATEWAY,
            "control_plane_error",
            format!("control-plane request failed: {err}"),
        )
    })?;

    match response {
        ControlResponse::Info {
            pid,
            mount_point,
            started_at,
            version,
            meta_backend,
            capabilities,
        } => Ok(Json(InstanceInfoResponse {
            pid,
            mount_point,
            started_at,
            version,
            meta_backend,
            capabilities,
        })),
        ControlResponse::Error { code, message } => Err(json_error(
            StatusCode::BAD_GATEWAY,
            "control_plane_error",
            format!("{code}: {message}"),
        )),
        other => Err(json_error(
            StatusCode::BAD_GATEWAY,
            "control_plane_error",
            format!("unexpected control-plane response: {other:?}"),
        )),
    }
}

pub fn json_error(
    status: StatusCode,
    code: &'static str,
    message: impl Into<String>,
) -> ApiErrorResponse {
    ApiErrorResponse {
        status,
        code,
        message: message.into(),
    }
}

#[derive(Debug)]
pub struct ApiErrorResponse {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl From<RegistryError> for ApiErrorResponse {
    fn from(err: RegistryError) -> Self {
        let status = match err.code() {
            "invalid_config" => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            code: err.code(),
            message: err.message().to_owned(),
        }
    }
}

impl IntoResponse for ApiErrorResponse {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        console::{AuthConfig, AuthMode, ConsoleState, registry::VolumeRegistry},
        control::runtime::RuntimeRegistry,
    };
    use std::path::PathBuf;

    #[test]
    fn health_response_uses_build_metadata_and_state() {
        let static_dir = PathBuf::from("/tmp/brewfs-console-dist");
        let state = ConsoleState {
            auth: AuthConfig::Disabled,
            static_dir: static_dir.clone(),
            registry: VolumeRegistry::new(static_dir.join("state")),
            runtime_registry: RuntimeRegistry::new(static_dir.join("runtime")),
            csi_dashboard: true,
        };

        let response = HealthResponse::from_state(&state, true);

        assert_eq!(response.service, "brewfs-console");
        assert_eq!(response.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(response.commit_short, env!("BREWFS_GIT_COMMIT_SHORT"));
        assert_eq!(response.auth_mode, AuthMode::Disabled);
        assert!(response.integrations.csi_dashboard);
        assert!(response.static_assets_available);
    }
}
