//! Versioned metadata lifecycle adapter hosted by the existing Console server.
//!
//! The Console bearer token is an administrator credential for this Manager's
//! configured catalog references. Catalog paths and backend credentials never
//! come from an HTTP request.

use super::{ConsoleState, api};
use axum::{
    Json, Router,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use std::path::PathBuf;

#[cfg(any(test, not(feature = "workspace-overlay")))]
pub(super) fn router(state_dir: PathBuf) -> Router<ConsoleState> {
    let router = Router::new().route("/capabilities", get(capabilities));
    #[cfg(feature = "workspace-overlay")]
    let router = enabled::routes(router, state_dir);
    #[cfg(not(feature = "workspace-overlay"))]
    let router = {
        let _ = state_dir;
        router
            .route("/volumes", get(unsupported).post(unsupported))
            .route("/volumes/{volume_id}", get(unsupported).delete(unsupported))
            .route("/volumes/{volume_id}/workspaces", get(unsupported))
            .route(
                "/volumes/{volume_id}/revisions/{layer_id}",
                get(unsupported),
            )
            .route(
                "/volumes/{volume_id}/workspaces/{workspace_id}",
                get(unsupported).delete(unsupported),
            )
            .route(
                "/volumes/{volume_id}/forks",
                axum::routing::post(unsupported),
            )
            .route("/operations/{operation_id}", get(unsupported))
    };
    unsupported_routes(router)
}

/// Recover persisted work before the real server accepts requests. The same
/// initialized handle is retained by the router for the server's lifetime.
pub(super) async fn initialized_router(state_dir: PathBuf) -> anyhow::Result<Router<ConsoleState>> {
    #[cfg(feature = "workspace-overlay")]
    {
        let router = Router::new().route("/capabilities", get(capabilities));
        Ok(unsupported_routes(
            enabled::initialized_routes(router, state_dir).await?,
        ))
    }
    #[cfg(not(feature = "workspace-overlay"))]
    {
        Ok(router(state_dir))
    }
}

fn unsupported_routes(router: Router<ConsoleState>) -> Router<ConsoleState> {
    router
        .route("/nodes", get(unsupported))
        .route(
            "/volumes/{volume_id}/workspaces/{workspace_id}/checkpoints",
            axum::routing::post(unsupported),
        )
        .route(
            "/volumes/{volume_id}/workspaces/{workspace_id}/attachments",
            axum::routing::post(unsupported),
        )
        .route(
            "/volumes/{volume_id}/workspaces/{workspace_id}/handoffs",
            axum::routing::post(unsupported),
        )
}

async fn capabilities() -> Json<serde_json::Value> {
    let enabled = cfg!(feature = "workspace-overlay");
    Json(serde_json::json!({
        "service": "brewfs-manager",
        "version": env!("CARGO_PKG_VERSION"),
        "api_version": "v1",
        "enabled": enabled,
        "formats": if enabled { vec!["workspace-v1"] } else { vec![] },
        "metadata_backends": if enabled { vec!["sqlite"] } else { vec![] },
        "max_fork_count": 100,
        "multi_replica": false,
        "authorization": "console_administrator_configured_catalogs",
        "persistent_operations": enabled,
        "workspace_delete": "lease_aware_logical_marker_shared_lowers_preserved",
        "physical_reclaim": false,
        "volume_delete": false,
        "mount": false,
        "drain": false,
        "checkpoint": false,
        "handoff": false,
        "remote_ready": false
    }))
}

async fn unsupported() -> Response {
    api::json_error(
        StatusCode::NOT_IMPLEMENTED,
        "unsupported",
        "this Manager build or API version does not support this operation",
    )
    .into_response()
}

#[cfg(feature = "workspace-overlay")]
mod enabled {
    use super::*;
    use crate::manager::{ForkRequest, Manager, ManagerError, VolumeRequest};
    use crate::workspace_overlay::ids::{LayerId, WorkspaceId};
    use axum::{
        Extension,
        body::Bytes,
        extract::{
            Path, Query,
            rejection::{JsonRejection, QueryRejection},
        },
        http::{HeaderMap, header},
        routing::post,
    };
    use serde::Deserialize;
    use std::sync::Arc;
    use tokio::sync::OnceCell;
    use uuid::Uuid;

    #[derive(Clone)]
    struct ManagerHandle {
        state_dir: PathBuf,
        manager: Arc<OnceCell<Arc<Manager>>>,
    }

    impl ManagerHandle {
        async fn get(&self) -> Result<&Arc<Manager>, api::ApiErrorResponse> {
            self.manager
                .get_or_try_init(|| async { Manager::open(&self.state_dir).await.map(Arc::new) })
                .await
                .map_err(manager_error)
        }
    }

    #[cfg(test)]
    pub(super) fn routes(router: Router<ConsoleState>, state_dir: PathBuf) -> Router<ConsoleState> {
        with_handle(
            router,
            ManagerHandle {
                state_dir,
                manager: Arc::new(OnceCell::new()),
            },
        )
    }

    pub(super) async fn initialized_routes(
        router: Router<ConsoleState>,
        state_dir: PathBuf,
    ) -> Result<Router<ConsoleState>, ManagerError> {
        let manager = Manager::open(&state_dir).await?;
        Ok(with_handle(
            router,
            ManagerHandle {
                state_dir,
                manager: Arc::new(OnceCell::new_with(Some(Arc::new(manager)))),
            },
        ))
    }

    fn with_handle(router: Router<ConsoleState>, handle: ManagerHandle) -> Router<ConsoleState> {
        router
            .route("/volumes", get(list_volumes).post(create_volume))
            .route(
                "/volumes/{volume_id}",
                get(inspect_volume).delete(unsupported),
            )
            .route("/volumes/{volume_id}/workspaces", get(list_workspaces))
            .route(
                "/volumes/{volume_id}/revisions/{layer_id}",
                get(inspect_revision),
            )
            .route(
                "/volumes/{volume_id}/workspaces/{workspace_id}",
                get(inspect_workspace).delete(delete_workspace),
            )
            .route("/volumes/{volume_id}/forks", post(fork))
            .route("/operations/{operation_id}", get(operation))
            .layer(Extension(handle))
    }

    fn invalid(message: &'static str) -> api::ApiErrorResponse {
        api::json_error(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    fn manager_error(error: ManagerError) -> api::ApiErrorResponse {
        let status = match error.code {
            "invalid_request" => StatusCode::BAD_REQUEST,
            "unsupported" => StatusCode::NOT_IMPLEMENTED,
            "not_found" => StatusCode::NOT_FOUND,
            "conflict" | "idempotency_conflict" => StatusCode::CONFLICT,
            "unavailable" | "recovery_required" => StatusCode::SERVICE_UNAVAILABLE,
            _ => {
                return api::json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "Manager storage operation failed",
                );
            }
        };
        api::json_error(status, error.code, error.message)
    }

    fn request_key(headers: &HeaderMap) -> Result<&str, api::ApiErrorResponse> {
        let mut values = headers.get_all("Idempotency-Key").iter();
        let key = values
            .next()
            .and_then(|value| value.to_str().ok())
            .filter(|key| {
                !key.is_empty() && key.len() <= 128 && key.bytes().all(|b| b.is_ascii_graphic())
            })
            .ok_or_else(|| {
                invalid("one Idempotency-Key of 1 to 128 visible ASCII characters is required")
            })?;
        if values.next().is_some() {
            return Err(invalid("only one Idempotency-Key is allowed"));
        }
        Ok(key)
    }

    fn uuid(value: &str) -> Result<Uuid, api::ApiErrorResponse> {
        Uuid::parse_str(value).map_err(|_| invalid("resource ID must be a UUID"))
    }

    fn json<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, api::ApiErrorResponse> {
        body.map(|Json(value)| value)
            .map_err(|_| invalid("request body does not match this API's JSON schema"))
    }

    async fn create_volume(
        Extension(handle): Extension<ManagerHandle>,
        headers: HeaderMap,
        body: Result<Json<VolumeRequest>, JsonRejection>,
    ) -> Result<Response, api::ApiErrorResponse> {
        let key = request_key(&headers)?;
        let request = json(body)?;
        let operation = handle
            .get()
            .await?
            .create_volume(key, request)
            .await
            .map_err(manager_error)?;
        let location = format!("/api/v1/operations/{}", operation.id);
        Ok((
            StatusCode::ACCEPTED,
            [(header::LOCATION, location)],
            Json(operation),
        )
            .into_response())
    }

    async fn list_volumes(
        Extension(handle): Extension<ManagerHandle>,
    ) -> Result<Response, api::ApiErrorResponse> {
        let volumes = handle
            .get()
            .await?
            .list_volumes()
            .await
            .map_err(manager_error)?;
        Ok(Json(serde_json::json!({"volumes": volumes})).into_response())
    }

    async fn inspect_volume(
        Extension(handle): Extension<ManagerHandle>,
        Path(id): Path<String>,
    ) -> Result<Response, api::ApiErrorResponse> {
        let id = uuid(&id)?;
        let volume = handle
            .get()
            .await?
            .inspect_volume(id)
            .await
            .map_err(manager_error)?;
        Ok(Json(volume).into_response())
    }

    async fn inspect_revision(
        Extension(handle): Extension<ManagerHandle>,
        Path((volume, layer)): Path<(String, String)>,
    ) -> Result<Response, api::ApiErrorResponse> {
        let volume = uuid(&volume)?;
        let layer = LayerId::from(uuid(&layer)?);
        let revision = handle
            .get()
            .await?
            .inspect_revision(volume, layer)
            .await
            .map_err(manager_error)?;
        Ok(Json(revision).into_response())
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct PageQuery {
        #[serde(default)]
        after: Option<Uuid>,
        #[serde(default = "page_limit")]
        limit: usize,
    }

    fn page_limit() -> usize {
        50
    }

    async fn list_workspaces(
        Extension(handle): Extension<ManagerHandle>,
        Path(id): Path<String>,
        query: Result<Query<PageQuery>, QueryRejection>,
    ) -> Result<Response, api::ApiErrorResponse> {
        let id = uuid(&id)?;
        let Query(query) = query.map_err(|_| invalid("invalid pagination query"))?;
        if query.limit == 0 || query.limit > 100 {
            return Err(invalid("limit must be between 1 and 100"));
        }
        let mut records = handle
            .get()
            .await?
            .list_workspaces(id)
            .await
            .map_err(manager_error)?;
        records.sort_by_key(|record| record.workspace_id.to_string());
        let start = match query.after {
            Some(after) => records
                .iter()
                .position(|record| record.workspace_id.to_string() == after.to_string())
                .map(|index| index + 1)
                .ok_or_else(|| invalid("pagination cursor is not present in this volume"))?,
            None => 0,
        };
        let end = start.saturating_add(query.limit).min(records.len());
        let next_after = if end < records.len() {
            records
                .get(end - 1)
                .map(|record| record.workspace_id.to_string())
        } else {
            None
        };
        Ok(
            Json(serde_json::json!({"workspaces": &records[start..end], "next_after": next_after}))
                .into_response(),
        )
    }

    async fn inspect_workspace(
        Extension(handle): Extension<ManagerHandle>,
        Path((volume, workspace)): Path<(String, String)>,
    ) -> Result<Response, api::ApiErrorResponse> {
        let volume = uuid(&volume)?;
        let workspace = WorkspaceId::from(uuid(&workspace)?);
        let workspace = handle
            .get()
            .await?
            .inspect_workspace(volume, workspace)
            .await
            .map_err(manager_error)?;
        Ok(Json(workspace).into_response())
    }

    async fn fork(
        Extension(handle): Extension<ManagerHandle>,
        Path(volume): Path<String>,
        headers: HeaderMap,
        body: Result<Json<ForkRequest>, JsonRejection>,
    ) -> Result<Response, api::ApiErrorResponse> {
        let volume = uuid(&volume)?;
        let key = request_key(&headers)?;
        let request = json(body)?;
        let operation = handle
            .get()
            .await?
            .fork(volume, key, request)
            .await
            .map_err(manager_error)?;
        let location = format!("/api/v1/operations/{}", operation.id);
        Ok((
            StatusCode::ACCEPTED,
            [(header::LOCATION, location)],
            Json(operation),
        )
            .into_response())
    }

    async fn delete_workspace(
        Extension(handle): Extension<ManagerHandle>,
        Path((volume, workspace)): Path<(String, String)>,
        headers: HeaderMap,
        query: Result<Query<EmptyQuery>, QueryRejection>,
        body: Bytes,
    ) -> Result<Response, api::ApiErrorResponse> {
        let volume = uuid(&volume)?;
        let workspace = WorkspaceId::from(uuid(&workspace)?);
        let key = request_key(&headers)?;
        query.map_err(|_| invalid("workspace deletion does not accept query options or force"))?;
        if !body.is_empty() {
            return Err(invalid("workspace deletion does not accept a request body"));
        }
        let operation = handle
            .get()
            .await?
            .delete_workspace(volume, workspace, key)
            .await
            .map_err(manager_error)?;
        let location = format!("/api/v1/operations/{}", operation.id);
        Ok((
            StatusCode::ACCEPTED,
            [(header::LOCATION, location)],
            Json(operation),
        )
            .into_response())
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct EmptyQuery {}

    async fn operation(
        Extension(handle): Extension<ManagerHandle>,
        Path(id): Path<String>,
    ) -> Result<Response, api::ApiErrorResponse> {
        let id = uuid(&id)?;
        let operation = handle
            .get()
            .await?
            .operation(id)
            .await
            .map_err(manager_error)?;
        Ok(Json(operation).into_response())
    }
}
