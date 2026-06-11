use super::{AuthConfig, ConsoleConfig, ConsoleState, api};
use axum::{
    Router,
    extract::State,
    http::{Request, StatusCode, Uri, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use std::path::{Path, PathBuf};
use tower_http::services::ServeDir;

pub fn build_router(config: ConsoleConfig) -> Router {
    let state = ConsoleState {
        auth: config.auth.clone(),
        static_dir: config.static_dir.clone(),
        registry: super::registry::VolumeRegistry::new(config.state_dir.clone()),
        runtime_registry: crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone()),
        csi_dashboard: config.csi_dashboard,
    };
    let api = Router::new()
        .route("/health", get(api::health))
        .route("/volumes", get(api::list_volumes).post(api::create_volume))
        .route("/instances", get(api::list_instances))
        .route("/instances/{pid}", get(api::get_instance_info))
        .fallback(api_not_found)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_auth,
        ));
    Router::new()
        .nest("/api", api)
        .nest_service("/assets", ServeDir::new(config.static_dir.join("assets")))
        .fallback(get(static_or_spa))
        .with_state(state)
}

pub async fn serve(config: ConsoleConfig) -> anyhow::Result<()> {
    let listen = config.listen;
    let app = build_router(config);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    println!("brewfs console listening on http://{listen}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn require_api_auth(
    State(state): State<ConsoleState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    match &state.auth {
        AuthConfig::Disabled => next.run(request).await,
        AuthConfig::Token { .. } => {
            let token = request
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "));
            if token.is_some_and(|token| state.auth.accepts_bearer(token)) {
                next.run(request).await
            } else {
                json_error(
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    "missing or invalid bearer token",
                )
                .into_response()
            }
        }
    }
}

async fn api_not_found() -> Response {
    json_error(StatusCode::NOT_FOUND, "not_found", "api route not found").into_response()
}

async fn static_or_spa(State(state): State<ConsoleState>, uri: Uri) -> Response {
    if uri.path() == "/api" || uri.path().starts_with("/api/") {
        return api_not_found().await;
    }

    if let Some(path) = static_file_path(&state.static_dir, uri.path()) {
        if tokio::fs::metadata(&path)
            .await
            .is_ok_and(|metadata| metadata.is_file())
        {
            match tokio::fs::read(&path).await {
                Ok(bytes) => {
                    return (
                        [(header::CONTENT_TYPE, content_type_for_path(&path))],
                        bytes,
                    )
                        .into_response();
                }
                Err(_) => return StatusCode::NOT_FOUND.into_response(),
            }
        }
    }

    spa_index(&state).await.into_response()
}

async fn spa_index(state: &ConsoleState) -> Result<Html<String>, StatusCode> {
    let html = tokio::fs::read_to_string(state.static_dir.join("index.html"))
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Html(html))
}

fn static_file_path(static_dir: &Path, request_path: &str) -> Option<PathBuf> {
    let mut path = PathBuf::from(static_dir);
    let relative = request_path.trim_start_matches('/');
    if relative.is_empty() {
        path.push("index.html");
        return Some(path);
    }
    for segment in relative.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return None;
        }
        path.push(segment);
    }
    Some(path)
}

fn content_type_for_path(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("css") => "text/css; charset=utf-8",
        Some("html") => "text/html; charset=utf-8",
        Some("ico") => "image/x-icon",
        Some("js") => "text/javascript; charset=utf-8",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("svg") => "image/svg+xml",
        Some("webmanifest") => "application/manifest+json",
        _ => "application/octet-stream",
    }
}

fn json_error(
    status: StatusCode,
    code: &'static str,
    message: impl Into<String>,
) -> api::ApiErrorResponse {
    api::json_error(status, code, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::console::{AuthConfig, ConsoleConfig};
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use std::net::SocketAddr;
    use tempfile::tempdir;
    use tower::ServiceExt;

    fn test_config(static_dir: &std::path::Path, auth: AuthConfig) -> ConsoleConfig {
        ConsoleConfig {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            state_dir: static_dir.join("state"),
            runtime_dir: static_dir.join("runtime"),
            static_dir: static_dir.to_path_buf(),
            auth,
            csi_dashboard: false,
        }
    }

    #[tokio::test]
    async fn health_route_returns_json() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["service"], "brewfs-console");
        assert_eq!(value["auth_mode"], "disabled");
    }

    #[tokio::test]
    async fn static_fallback_serves_index_html() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/filesystems")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(body.as_ref(), b"<div id=\"root\"></div>");
    }

    #[tokio::test]
    async fn token_auth_protects_api_routes() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(
            dir.path(),
            AuthConfig::Token {
                token: "secret".into(),
            },
        ));

        let missing_auth = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_auth.status(), StatusCode::UNAUTHORIZED);

        let wrong_auth = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .header(header::AUTHORIZATION, "Bearer wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_auth.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn unknown_api_route_returns_json_404() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "not_found");
    }

    #[tokio::test]
    async fn token_auth_protects_unknown_api_routes() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(
            dir.path(),
            AuthConfig::Token {
                token: "secret".into(),
            },
        ));

        let missing_auth = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_auth.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/missing")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn root_static_file_is_served_before_spa_fallback() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        std::fs::write(dir.path().join("favicon.ico"), "icon").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/favicon.ico")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(body.as_ref(), b"icon");
    }

    #[tokio::test]
    async fn volumes_api_creates_and_lists_redacted_registry_entries() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));

        let create_body = serde_json::json!({
            "name": "dev-local",
            "description": "local development",
            "labels": { "env": "dev" },
            "mount_config": {
                "mount_point": "/mnt/brewfs",
                "data_backend": "local-fs",
                "data_dir": "/var/lib/brewfs/data",
                "meta_backend": "sqlx",
                "meta_url": "postgres://brewfs:secret@db.example/brewfs",
                "chunk_size": 67108864,
                "block_size": 4194304
            }
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/volumes")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(create_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(created["name"], "dev-local");
        assert_eq!(
            created["mount_config"]["meta_url_redacted"],
            "postgres://brewfs:<redacted>@db.example/brewfs"
        );
        assert!(!String::from_utf8_lossy(&body).contains("secret"));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/volumes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let listed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(listed["volumes"][0]["id"], created["id"]);
        assert!(!String::from_utf8_lossy(&body).contains("secret"));
    }

    #[tokio::test]
    async fn instances_api_lists_live_runtime_records() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let record = crate::control::runtime::InstanceRecord::new(
            std::process::id(),
            "/mnt/brewfs".to_string(),
            registry.socket_path(std::process::id()),
            chrono::Utc::now(),
        );
        registry.write_record(&record).await.unwrap();
        let app = build_router(config);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/instances")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["instances"][0]["pid"], std::process::id());
        assert_eq!(value["instances"][0]["mount_point"], "/mnt/brewfs");
    }

    #[tokio::test]
    async fn instance_detail_calls_control_plane_get_info() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let pid = std::process::id();
        let socket_path = registry.socket_path(pid);
        let _server =
            crate::control::server::ControlServer::bind(socket_path.clone(), GetInfoHandler)
                .await
                .unwrap();
        let record = crate::control::runtime::InstanceRecord::new(
            pid,
            "/mnt/brewfs".to_string(),
            socket_path,
            chrono::Utc::now(),
        );
        registry.write_record(&record).await.unwrap();
        let app = build_router(config);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/instances/{pid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["pid"], pid);
        assert_eq!(value["mount_point"], "/mnt/brewfs");
        assert_eq!(value["meta_backend"], "sqlx");
        assert_eq!(value["capabilities"]["namespace"], true);
    }

    #[tokio::test]
    async fn instance_detail_returns_404_for_missing_runtime_record() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/instances/999999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    struct GetInfoHandler;

    #[async_trait::async_trait]
    impl crate::control::server::ControlHandler for GetInfoHandler {
        async fn handle(
            &self,
            request: crate::control::protocol::ControlRequest,
        ) -> crate::control::protocol::ControlResponse {
            match request {
                crate::control::protocol::ControlRequest::GetInfo => {
                    let capabilities = crate::meta::store::MetaStoreCapabilities {
                        namespace: true,
                        file_data: true,
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
