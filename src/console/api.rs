use super::{
    AuthMode, ConsoleState,
    acl::AclAdapterError,
    csi::CsiAdapterError,
    registry::{
        CreateVolumeRequest, RegistryError, UpdateVolumeRequest,
        VolumeResponse as RegistryVolumeResponse,
    },
    trash::TrashAdapterError,
};
use crate::{
    control::{
        client::send_request,
        job::{JobOutcome, JobState},
        protocol::{ControlDirectoryEntry, ControlFileKind, ControlRequest, ControlResponse},
        runtime::InstanceRecord,
    },
    meta::store::MetaStoreCapabilities,
};
use axum::{
    Json,
    extract::{Path, Query, State},
};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::{path::Path as FsPath, time::Duration};

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
    pub volumes: Vec<ConsoleVolumeResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ConsoleVolumeResponse {
    #[serde(flatten)]
    pub volume: RegistryVolumeResponse,
    pub runtime: VolumeRuntimeResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct VolumeRuntimeResponse {
    pub mounted: bool,
    pub pid: Option<u32>,
    pub mount_point: Option<String>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RunGcJobRequest {
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct AcceptedJobResponse {
    pub job_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct JobStatusResponse {
    pub job_id: String,
    pub state: JobState,
    pub detail: Option<String>,
    pub outcome: Option<JobOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PathQuery {
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CsiResourceQuery {
    pub namespace: Option<String>,
    pub volume: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FileListResponse {
    pub path: String,
    pub entries: Vec<FileEntryResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FileEntryResponse {
    pub name: String,
    pub inode: i64,
    pub kind: &'static str,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime: String,
    pub has_acl: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FileStatResponse {
    pub path: String,
    pub inode: i64,
    pub kind: &'static str,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ReadLinkResponse {
    pub path: String,
    pub target: String,
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
    let runtime_records = list_runtime_records(&state).await?;
    let volumes = volumes
        .into_iter()
        .map(|volume| enrich_volume_response(volume, &runtime_records))
        .collect();
    Ok(Json(ListVolumesResponse { volumes }))
}

pub async fn create_volume(
    State(state): State<ConsoleState>,
    Json(request): Json<CreateVolumeRequest>,
) -> Result<(StatusCode, Json<ConsoleVolumeResponse>), ApiErrorResponse> {
    let volume = state
        .registry
        .create(request)
        .await
        .map_err(ApiErrorResponse::from)?;
    let runtime_records = list_runtime_records(&state).await?;
    let volume = enrich_volume_response(volume, &runtime_records);
    Ok((StatusCode::CREATED, Json(volume)))
}

pub async fn get_volume(
    State(state): State<ConsoleState>,
    Path(volume_id): Path<String>,
) -> Result<Json<ConsoleVolumeResponse>, ApiErrorResponse> {
    let volume = state
        .registry
        .get(&volume_id)
        .await
        .map_err(ApiErrorResponse::from)?;
    let runtime_records = list_runtime_records(&state).await?;
    let volume = enrich_volume_response(volume, &runtime_records);
    Ok(Json(volume))
}

pub async fn update_volume(
    State(state): State<ConsoleState>,
    Path(volume_id): Path<String>,
    Json(request): Json<UpdateVolumeRequest>,
) -> Result<Json<ConsoleVolumeResponse>, ApiErrorResponse> {
    let volume = state
        .registry
        .update(&volume_id, request)
        .await
        .map_err(ApiErrorResponse::from)?;
    let runtime_records = list_runtime_records(&state).await?;
    let volume = enrich_volume_response(volume, &runtime_records);
    Ok(Json(volume))
}

pub async fn delete_volume(
    State(state): State<ConsoleState>,
    Path(volume_id): Path<String>,
) -> Result<StatusCode, ApiErrorResponse> {
    state
        .registry
        .delete(&volume_id)
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_runtime_records(
    state: &ConsoleState,
) -> Result<Vec<InstanceRecord>, ApiErrorResponse> {
    state
        .runtime_registry
        .list_instances()
        .await
        .map_err(|err| {
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "runtime_registry_error",
                format!("failed to read runtime registry: {err}"),
            )
        })
}

fn enrich_volume_response(
    volume: RegistryVolumeResponse,
    runtime_records: &[InstanceRecord],
) -> ConsoleVolumeResponse {
    let runtime = volume
        .mount_config
        .mount_point
        .as_deref()
        .and_then(|mount_point| {
            runtime_records
                .iter()
                .find(|record| record.mount_point == mount_point)
        })
        .map(|record| VolumeRuntimeResponse {
            mounted: true,
            pid: Some(record.pid),
            mount_point: Some(record.mount_point.clone()),
            started_at: Some(record.started_at),
        })
        .unwrap_or(VolumeRuntimeResponse {
            mounted: false,
            pid: None,
            mount_point: None,
            started_at: None,
        });

    ConsoleVolumeResponse { volume, runtime }
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
    Path(requested_pid): Path<u32>,
) -> Result<Json<InstanceInfoResponse>, ApiErrorResponse> {
    let response =
        send_instance_control_request(&state, requested_pid, &ControlRequest::GetInfo).await?;

    match response {
        ControlResponse::Info {
            pid,
            mount_point,
            started_at,
            version,
            meta_backend,
            capabilities,
        } => {
            if pid != requested_pid {
                return Err(json_error(
                    StatusCode::BAD_GATEWAY,
                    "control_plane_error",
                    format!("control-plane pid mismatch: requested {requested_pid}, got {pid}"),
                ));
            }
            Ok(Json(InstanceInfoResponse {
                pid,
                mount_point,
                started_at,
                version,
                meta_backend,
                capabilities,
            }))
        }
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

pub async fn start_gc_job(
    State(state): State<ConsoleState>,
    Path(pid): Path<u32>,
    Json(request): Json<RunGcJobRequest>,
) -> Result<(StatusCode, Json<AcceptedJobResponse>), ApiErrorResponse> {
    let response = send_instance_control_request(
        &state,
        pid,
        &ControlRequest::RunGc {
            dry_run: request.dry_run,
        },
    )
    .await?;

    match response {
        ControlResponse::Accepted { job_id } => {
            Ok((StatusCode::ACCEPTED, Json(AcceptedJobResponse { job_id })))
        }
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

pub async fn get_job_status(
    State(state): State<ConsoleState>,
    Path((pid, requested_job_id)): Path<(u32, String)>,
) -> Result<Json<JobStatusResponse>, ApiErrorResponse> {
    let response = send_instance_control_request(
        &state,
        pid,
        &ControlRequest::GetJob {
            job_id: requested_job_id.clone(),
        },
    )
    .await?;

    match response {
        ControlResponse::JobStatus {
            job_id,
            state,
            detail,
            outcome,
        } => {
            if job_id != requested_job_id {
                return Err(json_error(
                    StatusCode::BAD_GATEWAY,
                    "control_plane_error",
                    format!(
                        "control-plane job id mismatch: requested {requested_job_id}, got {job_id}"
                    ),
                ));
            }
            Ok(Json(JobStatusResponse {
                job_id,
                state,
                detail,
                outcome,
            }))
        }
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

pub async fn list_files(
    State(state): State<ConsoleState>,
    Path(volume_id): Path<String>,
    Query(query): Query<PathQuery>,
) -> Result<Json<FileListResponse>, ApiErrorResponse> {
    let path = normalize_absolute_path(query.path.as_deref().unwrap_or("/"))?;
    let record = find_runtime_record_for_volume(&state, &volume_id).await?;
    let response = send_control_request(
        &record.socket_path,
        &ControlRequest::ListDirectory { path: path.clone() },
    )
    .await?;

    match response {
        ControlResponse::DirectoryListing {
            path: response_path,
            entries,
        } => {
            if response_path != path {
                return Err(path_mismatch_error(&path, &response_path));
            }
            Ok(Json(FileListResponse {
                path: response_path,
                entries: entries.into_iter().map(FileEntryResponse::from).collect(),
            }))
        }
        ControlResponse::Error { code, message } => Err(control_error_response(&code, &message)),
        other => Err(json_error(
            StatusCode::BAD_GATEWAY,
            "control_plane_error",
            format!("unexpected control-plane response: {other:?}"),
        )),
    }
}

pub async fn stat_file(
    State(state): State<ConsoleState>,
    Path(volume_id): Path<String>,
    Query(query): Query<PathQuery>,
) -> Result<Json<FileStatResponse>, ApiErrorResponse> {
    let path = normalize_absolute_path(query.path.as_deref().unwrap_or("/"))?;
    let record = find_runtime_record_for_volume(&state, &volume_id).await?;
    let response = send_control_request(
        &record.socket_path,
        &ControlRequest::StatPath { path: path.clone() },
    )
    .await?;

    match response {
        ControlResponse::PathMetadata {
            path: response_path,
            metadata,
        } => {
            if response_path != path {
                return Err(path_mismatch_error(&path, &response_path));
            }
            Ok(Json(FileStatResponse {
                path: response_path,
                inode: metadata.inode,
                kind: control_kind_name(metadata.kind),
                size: metadata.size,
                mode: metadata.mode,
                uid: metadata.uid,
                gid: metadata.gid,
                mtime: format_timestamp_ns(metadata.mtime_ns),
            }))
        }
        ControlResponse::Error { code, message } => Err(control_error_response(&code, &message)),
        other => Err(json_error(
            StatusCode::BAD_GATEWAY,
            "control_plane_error",
            format!("unexpected control-plane response: {other:?}"),
        )),
    }
}

pub async fn read_link(
    State(state): State<ConsoleState>,
    Path(volume_id): Path<String>,
    Query(query): Query<PathQuery>,
) -> Result<Json<ReadLinkResponse>, ApiErrorResponse> {
    let path = normalize_absolute_path(query.path.as_deref().unwrap_or("/"))?;
    let record = find_runtime_record_for_volume(&state, &volume_id).await?;
    let response = send_control_request(
        &record.socket_path,
        &ControlRequest::ReadLink { path: path.clone() },
    )
    .await?;

    match response {
        ControlResponse::SymlinkTarget {
            path: response_path,
            target,
        } => {
            if response_path != path {
                return Err(path_mismatch_error(&path, &response_path));
            }
            Ok(Json(ReadLinkResponse {
                path: response_path,
                target,
            }))
        }
        ControlResponse::Error { code, message } => Err(control_error_response(&code, &message)),
        other => Err(json_error(
            StatusCode::BAD_GATEWAY,
            "control_plane_error",
            format!("unexpected control-plane response: {other:?}"),
        )),
    }
}

pub async fn list_trash(
    State(state): State<ConsoleState>,
    Path(volume_id): Path<String>,
) -> Result<Json<super::trash::TrashList>, ApiErrorResponse> {
    let record = find_runtime_record_for_volume(&state, &volume_id).await?;
    state
        .trash_adapter
        .list(&volume_id, &record)
        .await
        .map(Json)
        .map_err(trash_adapter_error)
}

pub async fn restore_trash_entry(
    State(state): State<ConsoleState>,
    Path((volume_id, entry_id)): Path<(String, String)>,
) -> Result<Json<super::trash::TrashActionResponse>, ApiErrorResponse> {
    let record = find_runtime_record_for_volume(&state, &volume_id).await?;
    state
        .trash_adapter
        .restore(&volume_id, &entry_id, &record)
        .await
        .map(|()| Json(super::trash::TrashActionResponse { ok: true }))
        .map_err(trash_adapter_error)
}

pub async fn delete_trash_entry(
    State(state): State<ConsoleState>,
    Path((volume_id, entry_id)): Path<(String, String)>,
) -> Result<Json<super::trash::TrashActionResponse>, ApiErrorResponse> {
    let record = find_runtime_record_for_volume(&state, &volume_id).await?;
    state
        .trash_adapter
        .delete(&volume_id, &entry_id, &record)
        .await
        .map(|()| Json(super::trash::TrashActionResponse { ok: true }))
        .map_err(trash_adapter_error)
}

pub async fn get_acl(
    State(state): State<ConsoleState>,
    Path(volume_id): Path<String>,
    Query(query): Query<PathQuery>,
) -> Result<Json<super::acl::AclResponse>, ApiErrorResponse> {
    let path = normalize_absolute_path(query.path.as_deref().unwrap_or("/"))?;
    let record = ensure_acl_capability(&state, &volume_id).await?;
    state
        .acl_adapter
        .get(&volume_id, &path, &record)
        .await
        .map(Json)
        .map_err(acl_adapter_error)
}

pub async fn put_acl(
    State(state): State<ConsoleState>,
    Path(volume_id): Path<String>,
    Query(query): Query<PathQuery>,
    Json(request): Json<super::acl::AclResponse>,
) -> Result<Json<super::acl::AclResponse>, ApiErrorResponse> {
    let path = normalize_absolute_path(query.path.as_deref().unwrap_or("/"))?;
    let record = ensure_acl_capability(&state, &volume_id).await?;
    state
        .acl_adapter
        .put(&volume_id, &path, request, &record)
        .await
        .map(Json)
        .map_err(acl_adapter_error)
}

pub async fn delete_acl(
    State(state): State<ConsoleState>,
    Path(volume_id): Path<String>,
    Query(query): Query<PathQuery>,
) -> Result<Json<super::acl::AclActionResponse>, ApiErrorResponse> {
    let path = normalize_absolute_path(query.path.as_deref().unwrap_or("/"))?;
    let record = ensure_acl_capability(&state, &volume_id).await?;
    state
        .acl_adapter
        .delete(&volume_id, &path, &record)
        .await
        .map(|()| Json(super::acl::AclActionResponse { ok: true }))
        .map_err(acl_adapter_error)
}

pub async fn csi_summary(
    State(state): State<ConsoleState>,
) -> Result<Json<super::csi::CsiSummary>, ApiErrorResponse> {
    ensure_csi_dashboard_enabled(&state)?;
    state
        .csi_adapter
        .summary()
        .await
        .map(Json)
        .map_err(csi_adapter_error)
}

pub async fn csi_storageclasses(
    State(state): State<ConsoleState>,
) -> Result<Json<super::csi::CsiResourceList>, ApiErrorResponse> {
    ensure_csi_dashboard_enabled(&state)?;
    state
        .csi_adapter
        .storageclasses()
        .await
        .map(Json)
        .map_err(csi_adapter_error)
}

pub async fn csi_persistentvolumes(
    State(state): State<ConsoleState>,
) -> Result<Json<super::csi::CsiResourceList>, ApiErrorResponse> {
    ensure_csi_dashboard_enabled(&state)?;
    state
        .csi_adapter
        .persistentvolumes()
        .await
        .map(Json)
        .map_err(csi_adapter_error)
}

pub async fn csi_persistentvolumeclaims(
    State(state): State<ConsoleState>,
    Query(query): Query<CsiResourceQuery>,
) -> Result<Json<super::csi::CsiResourceList>, ApiErrorResponse> {
    ensure_csi_dashboard_enabled(&state)?;
    state
        .csi_adapter
        .persistentvolumeclaims(&query)
        .await
        .map(Json)
        .map_err(csi_adapter_error)
}

pub async fn csi_pods(
    State(state): State<ConsoleState>,
    Query(query): Query<CsiResourceQuery>,
) -> Result<Json<super::csi::CsiResourceList>, ApiErrorResponse> {
    ensure_csi_dashboard_enabled(&state)?;
    state
        .csi_adapter
        .pods(&query)
        .await
        .map(Json)
        .map_err(csi_adapter_error)
}

async fn send_instance_control_request(
    state: &ConsoleState,
    pid: u32,
    request: &ControlRequest,
) -> Result<ControlResponse, ApiErrorResponse> {
    let record = find_runtime_record(state, pid).await?;
    send_control_request(&record.socket_path, request).await
}

async fn find_runtime_record(
    state: &ConsoleState,
    pid: u32,
) -> Result<InstanceRecord, ApiErrorResponse> {
    state
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
        })
}

async fn find_runtime_record_for_volume(
    state: &ConsoleState,
    volume_id: &str,
) -> Result<InstanceRecord, ApiErrorResponse> {
    find_optional_runtime_record_for_volume(state, volume_id)
        .await?
        .ok_or_else(|| json_error(StatusCode::NOT_FOUND, "not_found", "volume not found"))
}

async fn find_optional_runtime_record_for_volume(
    state: &ConsoleState,
    volume_id: &str,
) -> Result<Option<InstanceRecord>, ApiErrorResponse> {
    let volume = match state.registry.get(volume_id).await {
        Ok(volume) => volume,
        Err(err) if err.code() == "not_found" => return Ok(None),
        Err(err) => return Err(ApiErrorResponse::from(err)),
    };
    let mount_point = volume.mount_config.mount_point.ok_or_else(|| {
        instance_unavailable("registered volume has no mount point; mount it before using runtime-backed console features")
    })?;

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
        .find(|record| record.mount_point == mount_point)
        .ok_or_else(|| {
            instance_unavailable(format!(
                "registered volume is not mounted at {mount_point}; mount it before using runtime-backed console features"
            ))
        })?;
    Ok(Some(record))
}

async fn ensure_acl_capability(
    state: &ConsoleState,
    volume_id: &str,
) -> Result<InstanceRecord, ApiErrorResponse> {
    let record = find_runtime_record_for_volume(state, volume_id).await?;
    let response = send_control_request(&record.socket_path, &ControlRequest::GetInfo).await?;
    match response {
        ControlResponse::Info {
            pid,
            mount_point,
            capabilities,
            ..
        } => {
            if pid != record.pid {
                return Err(json_error(
                    StatusCode::BAD_GATEWAY,
                    "control_plane_error",
                    format!(
                        "control-plane pid mismatch: requested {}, got {pid}",
                        record.pid
                    ),
                ));
            }
            if mount_point != record.mount_point {
                return Err(json_error(
                    StatusCode::BAD_GATEWAY,
                    "control_plane_error",
                    format!(
                        "control-plane mount mismatch: requested {}, got {mount_point}",
                        record.mount_point
                    ),
                ));
            }
            if capabilities.acl {
                Ok(record)
            } else {
                Err(unsupported(
                    "ACL is not supported by the mounted metadata backend",
                ))
            }
        }
        ControlResponse::Error { code, message } => Err(control_error_response(&code, &message)),
        other => Err(json_error(
            StatusCode::BAD_GATEWAY,
            "control_plane_error",
            format!("unexpected control-plane response: {other:?}"),
        )),
    }
}

async fn send_control_request(
    socket_path: &FsPath,
    request: &ControlRequest,
) -> Result<ControlResponse, ApiErrorResponse> {
    tokio::time::timeout(Duration::from_secs(2), send_request(socket_path, request))
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
        })
}

fn unsupported(message: impl Into<String>) -> ApiErrorResponse {
    json_error(StatusCode::UNPROCESSABLE_ENTITY, "unsupported", message)
}

fn unavailable(message: impl Into<String>) -> ApiErrorResponse {
    json_error(StatusCode::CONFLICT, "unavailable", message)
}

fn instance_unavailable(message: impl Into<String>) -> ApiErrorResponse {
    json_error(StatusCode::CONFLICT, "instance_unavailable", message)
}

fn ensure_csi_dashboard_enabled(state: &ConsoleState) -> Result<(), ApiErrorResponse> {
    if state.csi_dashboard {
        Ok(())
    } else {
        Err(unavailable("CSI dashboard is disabled"))
    }
}

fn csi_adapter_error(err: CsiAdapterError) -> ApiErrorResponse {
    match err {
        CsiAdapterError::Disabled => unavailable("CSI dashboard is disabled"),
        CsiAdapterError::Unsupported(message) => unsupported(message),
        CsiAdapterError::Unavailable(message) => {
            json_error(StatusCode::BAD_GATEWAY, "kubernetes_error", message)
        }
    }
}

fn trash_adapter_error(err: TrashAdapterError) -> ApiErrorResponse {
    match err {
        TrashAdapterError::Unsupported(message) => unsupported(message),
        TrashAdapterError::ControlPlane(message) => {
            json_error(StatusCode::BAD_GATEWAY, "control_plane_error", message)
        }
    }
}

fn acl_adapter_error(err: AclAdapterError) -> ApiErrorResponse {
    match err {
        AclAdapterError::InvalidRequest(message) => {
            json_error(StatusCode::BAD_REQUEST, "invalid_request", message)
        }
        AclAdapterError::Unsupported(message) => unsupported(message),
        AclAdapterError::ControlPlane(message) => {
            json_error(StatusCode::BAD_GATEWAY, "control_plane_error", message)
        }
    }
}

fn control_error_response(code: &str, message: &str) -> ApiErrorResponse {
    match code {
        "not_found" => json_error(StatusCode::NOT_FOUND, "not_found", message),
        "invalid_request" => json_error(StatusCode::BAD_REQUEST, "invalid_request", message),
        "not_directory" | "invalid_path" => {
            json_error(StatusCode::BAD_REQUEST, "invalid_path", message)
        }
        "unsupported" => unsupported(message),
        _ => json_error(
            StatusCode::BAD_GATEWAY,
            "control_plane_error",
            format!("{code}: {message}"),
        ),
    }
}

fn path_mismatch_error(requested_path: &str, response_path: &str) -> ApiErrorResponse {
    json_error(
        StatusCode::BAD_GATEWAY,
        "control_plane_error",
        format!("control-plane path mismatch: requested {requested_path}, got {response_path}"),
    )
}

fn normalize_absolute_path(path: &str) -> Result<String, ApiErrorResponse> {
    let path = path.trim();
    if path.is_empty() {
        return Ok("/".to_string());
    }
    if !path.starts_with('/') {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_path",
            "path must be absolute",
        ));
    }

    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part),
        }
    }

    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

impl From<ControlDirectoryEntry> for FileEntryResponse {
    fn from(entry: ControlDirectoryEntry) -> Self {
        Self {
            name: entry.name,
            inode: entry.inode,
            kind: control_kind_name(entry.kind),
            size: entry.size,
            mode: entry.mode,
            uid: entry.uid,
            gid: entry.gid,
            mtime: format_timestamp_ns(entry.mtime_ns),
            has_acl: entry.has_acl,
        }
    }
}

fn control_kind_name(kind: ControlFileKind) -> &'static str {
    match kind {
        ControlFileKind::File => "file",
        ControlFileKind::Directory => "directory",
        ControlFileKind::Symlink => "symlink",
    }
}

fn format_timestamp_ns(value: i64) -> String {
    let seconds = value.div_euclid(1_000_000_000);
    let nanos = value.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, nanos)
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_else(|| value.to_string())
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
            "not_found" => StatusCode::NOT_FOUND,
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
        console::{
            AuthConfig, AuthMode, ConsoleState,
            acl::{AclAdapter, AclEntry, AclResponse},
            csi::{CsiAdapter, CsiResourceList, CsiSummary},
            registry::{CreateVolumeMountConfig, CreateVolumeRequest, VolumeRegistry},
            trash::{TrashAdapter, TrashEntry, TrashList},
        },
        control::runtime::{InstanceRecord, RuntimeRegistry},
    };
    use std::collections::BTreeMap;
    use std::{path::PathBuf, sync::Arc};

    #[test]
    fn maps_control_invalid_request_to_bad_request() {
        let err = control_error_response("invalid_request", "ACL entry 1 is invalid");

        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "invalid_request");
        assert_eq!(err.message, "ACL entry 1 is invalid");
    }

    #[derive(Debug)]
    struct ReadyCsiAdapter;

    #[async_trait::async_trait]
    impl CsiAdapter for ReadyCsiAdapter {
        async fn summary(&self) -> Result<CsiSummary, super::super::csi::CsiAdapterError> {
            Ok(CsiSummary {
                storageclasses: 1,
                persistentvolumes: 2,
                persistentvolumeclaims: 3,
                pods: 4,
                unhealthy_mounts: 0,
            })
        }

        async fn storageclasses(
            &self,
        ) -> Result<CsiResourceList, super::super::csi::CsiAdapterError> {
            Ok(CsiResourceList::default())
        }

        async fn persistentvolumes(
            &self,
        ) -> Result<CsiResourceList, super::super::csi::CsiAdapterError> {
            Ok(CsiResourceList::default())
        }

        async fn persistentvolumeclaims(
            &self,
            _query: &CsiResourceQuery,
        ) -> Result<CsiResourceList, super::super::csi::CsiAdapterError> {
            Ok(CsiResourceList::default())
        }

        async fn pods(
            &self,
            _query: &CsiResourceQuery,
        ) -> Result<CsiResourceList, super::super::csi::CsiAdapterError> {
            Ok(CsiResourceList::default())
        }
    }

    #[derive(Debug)]
    struct ReadyTrashAdapter;

    #[async_trait::async_trait]
    impl TrashAdapter for ReadyTrashAdapter {
        async fn list(
            &self,
            _volume_id: &str,
            _runtime: &InstanceRecord,
        ) -> Result<TrashList, super::super::trash::TrashAdapterError> {
            Ok(TrashList {
                entries: vec![TrashEntry {
                    id: "trash-1".to_string(),
                    original_path: "/docs/report.txt".to_string(),
                    size: Some(42),
                    deleted_at: Some("2026-06-11T12:00:00Z".to_string()),
                }],
            })
        }

        async fn restore(
            &self,
            _volume_id: &str,
            _entry_id: &str,
            _runtime: &InstanceRecord,
        ) -> Result<(), super::super::trash::TrashAdapterError> {
            Ok(())
        }

        async fn delete(
            &self,
            _volume_id: &str,
            _entry_id: &str,
            _runtime: &InstanceRecord,
        ) -> Result<(), super::super::trash::TrashAdapterError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct ReadyAclAdapter;

    #[async_trait::async_trait]
    impl AclAdapter for ReadyAclAdapter {
        async fn get(
            &self,
            _volume_id: &str,
            _path: &str,
            _runtime: &InstanceRecord,
        ) -> Result<AclResponse, super::super::acl::AclAdapterError> {
            Ok(AclResponse {
                entries: vec![AclEntry {
                    scope: "access".to_string(),
                    tag: "user_obj".to_string(),
                    id: None,
                    perm: "rwx".to_string(),
                }],
            })
        }

        async fn put(
            &self,
            _volume_id: &str,
            _path: &str,
            request: AclResponse,
            _runtime: &InstanceRecord,
        ) -> Result<AclResponse, super::super::acl::AclAdapterError> {
            Ok(request)
        }

        async fn delete(
            &self,
            _volume_id: &str,
            _path: &str,
            _runtime: &InstanceRecord,
        ) -> Result<(), super::super::acl::AclAdapterError> {
            Ok(())
        }
    }

    #[test]
    fn health_response_uses_build_metadata_and_state() {
        let static_dir = PathBuf::from("/tmp/brewfs-console-dist");
        let state = ConsoleState {
            auth: AuthConfig::Disabled,
            static_dir: static_dir.clone(),
            registry: VolumeRegistry::new(static_dir.join("state")),
            runtime_registry: RuntimeRegistry::new(static_dir.join("runtime")),
            csi_dashboard: true,
            csi_adapter: Arc::new(ReadyCsiAdapter),
            trash_adapter: Arc::new(ReadyTrashAdapter),
            acl_adapter: Arc::new(ReadyAclAdapter),
        };

        let response = HealthResponse::from_state(&state, true);

        assert_eq!(response.service, "brewfs-console");
        assert_eq!(response.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(response.commit_short, env!("BREWFS_GIT_COMMIT_SHORT"));
        assert_eq!(response.auth_mode, AuthMode::Disabled);
        assert!(response.integrations.csi_dashboard);
        assert!(response.static_assets_available);
    }

    #[tokio::test]
    async fn csi_summary_uses_state_adapter() {
        let static_dir = PathBuf::from("/tmp/brewfs-console-dist");
        let state = ConsoleState {
            auth: AuthConfig::Disabled,
            static_dir: static_dir.clone(),
            registry: VolumeRegistry::new(static_dir.join("state")),
            runtime_registry: RuntimeRegistry::new(static_dir.join("runtime")),
            csi_dashboard: true,
            csi_adapter: Arc::new(ReadyCsiAdapter),
            trash_adapter: Arc::new(ReadyTrashAdapter),
            acl_adapter: Arc::new(ReadyAclAdapter),
        };

        let Json(response) = csi_summary(State(state)).await.unwrap();

        assert_eq!(response.storageclasses, 1);
        assert_eq!(response.pods, 4);
    }

    #[tokio::test]
    async fn csi_summary_respects_disabled_dashboard_even_with_ready_adapter() {
        let static_dir = PathBuf::from("/tmp/brewfs-console-dist");
        let state = ConsoleState {
            auth: AuthConfig::Disabled,
            static_dir: static_dir.clone(),
            registry: VolumeRegistry::new(static_dir.join("state")),
            runtime_registry: RuntimeRegistry::new(static_dir.join("runtime")),
            csi_dashboard: false,
            csi_adapter: Arc::new(ReadyCsiAdapter),
            trash_adapter: Arc::new(ReadyTrashAdapter),
            acl_adapter: Arc::new(ReadyAclAdapter),
        };

        let err = csi_summary(State(state)).await.unwrap_err();

        assert_eq!(err.status, StatusCode::CONFLICT);
        assert_eq!(err.code, "unavailable");
    }

    #[tokio::test]
    async fn list_trash_uses_state_adapter_for_mounted_volume() {
        let dir = tempfile::tempdir().unwrap();
        let registry = VolumeRegistry::new(dir.path().join("state"));
        let runtime_registry = RuntimeRegistry::new(dir.path().join("runtime"));
        let volume = registry
            .create(CreateVolumeRequest {
                name: "dev-local".to_string(),
                description: None,
                labels: BTreeMap::new(),
                mount_config: CreateVolumeMountConfig {
                    mount_point: Some("/mnt/brewfs".to_string()),
                    data_backend: "local-fs".to_string(),
                    data_dir: None,
                    meta_backend: "sqlx".to_string(),
                    meta_url: None,
                    chunk_size: None,
                    block_size: None,
                },
            })
            .await
            .unwrap();
        let record = InstanceRecord::new(
            std::process::id(),
            "/mnt/brewfs".to_string(),
            runtime_registry.socket_path(std::process::id()),
            chrono::Utc::now(),
        );
        runtime_registry.write_record(&record).await.unwrap();
        let state = ConsoleState {
            auth: AuthConfig::Disabled,
            static_dir: dir.path().join("static"),
            registry,
            runtime_registry,
            csi_dashboard: false,
            csi_adapter: Arc::new(ReadyCsiAdapter),
            trash_adapter: Arc::new(ReadyTrashAdapter),
            acl_adapter: Arc::new(ReadyAclAdapter),
        };

        let Json(response) = list_trash(State(state), Path(volume.id)).await.unwrap();

        assert_eq!(response.entries[0].id, "trash-1");
        assert_eq!(response.entries[0].original_path, "/docs/report.txt");
    }

    #[tokio::test]
    async fn get_acl_uses_state_adapter_after_capability_check() {
        let (_dir, _server, state, volume_id) = acl_ready_state().await;

        let Json(response) = get_acl(
            State(state),
            Path(volume_id),
            Query(PathQuery {
                path: Some("/".to_string()),
            }),
        )
        .await
        .unwrap();

        assert_eq!(response.entries[0].tag, "user_obj");
        assert_eq!(response.entries[0].perm, "rwx");
    }

    #[tokio::test]
    async fn put_acl_uses_state_adapter_after_capability_check() {
        let (_dir, _server, state, volume_id) = acl_ready_state().await;
        let request = AclResponse {
            entries: vec![
                AclEntry {
                    scope: "access".to_string(),
                    tag: "user_obj".to_string(),
                    id: None,
                    perm: "rwx".to_string(),
                },
                AclEntry {
                    scope: "access".to_string(),
                    tag: "group_obj".to_string(),
                    id: None,
                    perm: "r-x".to_string(),
                },
                AclEntry {
                    scope: "access".to_string(),
                    tag: "other".to_string(),
                    id: None,
                    perm: "---".to_string(),
                },
                AclEntry {
                    scope: "access".to_string(),
                    tag: "group".to_string(),
                    id: Some(1_000),
                    perm: "r-x".to_string(),
                },
            ],
        };

        let Json(response) = put_acl(
            State(state),
            Path(volume_id),
            Query(PathQuery {
                path: Some("/docs".to_string()),
            }),
            Json(request.clone()),
        )
        .await
        .unwrap();

        assert_eq!(response, request);
    }

    #[tokio::test]
    async fn delete_acl_uses_state_adapter_after_capability_check() {
        let (_dir, _server, state, volume_id) = acl_ready_state().await;

        let Json(response) = delete_acl(
            State(state),
            Path(volume_id),
            Query(PathQuery {
                path: Some("/docs".to_string()),
            }),
        )
        .await
        .unwrap();

        assert!(response.ok);
    }

    async fn acl_ready_state() -> (
        tempfile::TempDir,
        crate::control::server::ControlServer,
        ConsoleState,
        String,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let registry = VolumeRegistry::new(dir.path().join("state"));
        let runtime_registry = RuntimeRegistry::new(dir.path().join("runtime"));
        let volume = registry
            .create(CreateVolumeRequest {
                name: "dev-local".to_string(),
                description: None,
                labels: BTreeMap::new(),
                mount_config: CreateVolumeMountConfig {
                    mount_point: Some("/mnt/brewfs".to_string()),
                    data_backend: "local-fs".to_string(),
                    data_dir: None,
                    meta_backend: "sqlx".to_string(),
                    meta_url: None,
                    chunk_size: None,
                    block_size: None,
                },
            })
            .await
            .unwrap();
        let pid = std::process::id();
        let socket_path = runtime_registry.socket_path(pid);
        let server =
            crate::control::server::ControlServer::bind(socket_path.clone(), AclCapabilityHandler)
                .await
                .unwrap();
        let record = InstanceRecord::new(
            pid,
            "/mnt/brewfs".to_string(),
            socket_path,
            chrono::Utc::now(),
        );
        runtime_registry.write_record(&record).await.unwrap();
        let state = ConsoleState {
            auth: AuthConfig::Disabled,
            static_dir: dir.path().join("static"),
            registry,
            runtime_registry,
            csi_dashboard: false,
            csi_adapter: Arc::new(ReadyCsiAdapter),
            trash_adapter: Arc::new(ReadyTrashAdapter),
            acl_adapter: Arc::new(ReadyAclAdapter),
        };
        (dir, server, state, volume.id)
    }

    struct AclCapabilityHandler;

    #[async_trait::async_trait]
    impl crate::control::server::ControlHandler for AclCapabilityHandler {
        async fn handle(
            &self,
            request: crate::control::protocol::ControlRequest,
        ) -> crate::control::protocol::ControlResponse {
            match request {
                crate::control::protocol::ControlRequest::GetInfo => {
                    let capabilities = crate::meta::store::MetaStoreCapabilities {
                        acl: true,
                        ..Default::default()
                    };
                    crate::control::protocol::ControlResponse::Info {
                        pid: std::process::id(),
                        mount_point: "/mnt/brewfs".to_string(),
                        started_at: 1_786_000_000_000,
                        version: "0.1.0-test".to_string(),
                        meta_backend: "sqlx".to_string(),
                        capabilities,
                    }
                }
                other => crate::control::protocol::ControlResponse::Error {
                    code: "unexpected".to_string(),
                    message: format!("unexpected request: {other:?}"),
                },
            }
        }
    }
}
