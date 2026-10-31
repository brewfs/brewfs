use super::{AuthConfig, ConsoleConfig, ConsoleState, api};
use axum::{
    Router,
    extract::State,
    http::{Method, Request, StatusCode, Uri, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
};
use std::path::{Path, PathBuf};
use tower_http::services::ServeDir;

#[cfg(test)]
pub fn build_router(config: ConsoleConfig) -> Router {
    let manager_router = super::manager_api::router(config.state_dir.clone());
    build_router_with_manager_router(config, manager_router)
}

fn build_router_with_manager_router(
    config: ConsoleConfig,
    manager_router: Router<ConsoleState>,
) -> Router {
    let state = ConsoleState {
        auth: config.auth.clone(),
        static_dir: config.static_dir.clone(),
        registry: super::registry::VolumeRegistry::new(config.state_dir.clone()),
        runtime_registry: crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone()),
        csi_dashboard: config.csi.enabled,
        csi_adapter: super::csi::default_csi_adapter(config.csi.clone()),
        trash_adapter: super::trash::default_trash_adapter(),
        acl_adapter: super::acl::default_acl_adapter(),
    };
    let api = Router::new()
        .nest("/v1", manager_router)
        .route("/health", get(api::health))
        .route("/volumes", get(api::list_volumes).post(api::create_volume))
        .route(
            "/volumes/{volume_id}",
            get(api::get_volume)
                .patch(api::update_volume)
                .delete(api::delete_volume),
        )
        .route("/volumes/{volume_id}/files", get(api::list_files))
        .route("/volumes/{volume_id}/files/stat", get(api::stat_file))
        .route("/volumes/{volume_id}/files/readlink", get(api::read_link))
        .route("/volumes/{volume_id}/trash", get(api::list_trash))
        .route(
            "/volumes/{volume_id}/trash/{entry_id}/restore",
            post(api::restore_trash_entry),
        )
        .route(
            "/volumes/{volume_id}/trash/{entry_id}",
            delete(api::delete_trash_entry),
        )
        .route(
            "/volumes/{volume_id}/acl",
            get(api::get_acl).put(api::put_acl).delete(api::delete_acl),
        )
        .route("/instances", get(api::list_instances))
        .route("/instances/{pid}", get(api::get_instance_info))
        .route("/instances/{pid}/jobs/gc", post(api::start_gc_job))
        .route("/instances/{pid}/jobs/{job_id}", get(api::get_job_status))
        .route("/csi/summary", get(api::csi_summary))
        .route("/csi/storageclasses", get(api::csi_storageclasses))
        .route("/csi/persistentvolumes", get(api::csi_persistentvolumes))
        .route(
            "/csi/persistentvolumeclaims",
            get(api::csi_persistentvolumeclaims),
        )
        .route("/csi/pods", get(api::csi_pods))
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
    let listener = tokio::net::TcpListener::bind(listen).await?;
    serve_listener(listener, config).await
}

async fn serve_listener(
    listener: tokio::net::TcpListener,
    config: ConsoleConfig,
) -> anyhow::Result<()> {
    let listen = listener.local_addr()?;
    let manager_router = super::manager_api::initialized_router(config.state_dir.clone()).await?;
    let app = build_router_with_manager_router(config, manager_router);
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
        AuthConfig::Disabled => {
            audit_mutating_api_request(&state.auth, request.method(), request.uri());
            next.run(request).await
        }
        AuthConfig::Token { .. } => {
            let token = request
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "));
            if token.is_some_and(|token| state.auth.accepts_bearer(token)) {
                audit_mutating_api_request(&state.auth, request.method(), request.uri());
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

fn audit_mutating_api_request(auth: &AuthConfig, method: &Method, uri: &Uri) {
    if !is_mutating_api_method(method) {
        return;
    }

    tracing::info!(
        target: "brewfs_console_audit",
        user = audit_identity_label(auth),
        method = %method,
        path = uri.path(),
        "console mutating API request"
    );
}

fn is_mutating_api_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

fn audit_identity_label(auth: &AuthConfig) -> &'static str {
    match auth {
        AuthConfig::Disabled => "dev-no-auth",
        AuthConfig::Token { .. } => "token",
    }
}

async fn api_not_found() -> Response {
    json_error(StatusCode::NOT_FOUND, "not_found", "api route not found").into_response()
}

async fn static_or_spa(State(state): State<ConsoleState>, uri: Uri) -> Response {
    if uri.path() == "/api" || uri.path().starts_with("/api/") {
        return api_not_found().await;
    }

    if let Some(path) = static_file_path(&state.static_dir, uri.path())
        && tokio::fs::metadata(&path)
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
    use crate::console::{AuthConfig, ConsoleConfig, ConsoleCsiConfig};
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use std::net::SocketAddr;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tower::ServiceExt;

    #[test]
    fn audit_treats_only_write_methods_as_mutating() {
        assert!(!is_mutating_api_method(&axum::http::Method::GET));
        assert!(!is_mutating_api_method(&axum::http::Method::HEAD));
        assert!(is_mutating_api_method(&axum::http::Method::POST));
        assert!(is_mutating_api_method(&axum::http::Method::PUT));
        assert!(is_mutating_api_method(&axum::http::Method::PATCH));
        assert!(is_mutating_api_method(&axum::http::Method::DELETE));
    }

    #[test]
    fn audit_identity_labels_do_not_include_token_values() {
        assert_eq!(audit_identity_label(&AuthConfig::Disabled), "dev-no-auth");
        assert_eq!(
            audit_identity_label(&AuthConfig::Token {
                token: "secret-token".into(),
            }),
            "token"
        );
    }

    fn test_config(static_dir: &std::path::Path, auth: AuthConfig) -> ConsoleConfig {
        ConsoleConfig {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            state_dir: static_dir.join("state"),
            runtime_dir: static_dir.join("runtime"),
            static_dir: static_dir.to_path_buf(),
            auth,
            csi: ConsoleCsiConfig {
                enabled: false,
                kubeconfig: None,
                driver_name: crate::console::csi::DEFAULT_DRIVER_NAME.to_string(),
            },
        }
    }

    async fn build_mounted_app_with_handler<H>(
        static_dir: &std::path::Path,
        handler: H,
    ) -> (Router, crate::control::server::ControlServer)
    where
        H: crate::control::server::ControlHandler,
    {
        std::fs::write(static_dir.join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(static_dir, AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let pid = std::process::id();
        let socket_path = registry.socket_path(pid);
        let server = crate::control::server::ControlServer::bind(socket_path.clone(), handler)
            .await
            .unwrap();
        let record = crate::control::runtime::InstanceRecord::new(
            pid,
            "/mnt/brewfs".to_string(),
            socket_path,
            chrono::Utc::now(),
        );
        registry.write_record(&record).await.unwrap();
        (build_router(config), server)
    }

    #[tokio::test]
    async fn tcp_listener_serves_health_json() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_listener(listener, config));

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut raw = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            stream.read_to_string(&mut raw),
        )
        .await
        .unwrap()
        .unwrap();

        server.abort();
        let _ = server.await;

        assert!(raw.starts_with("HTTP/1.1 200 OK"), "{raw}");
        let body = raw.split("\r\n\r\n").nth(1).unwrap();
        let value: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(value["service"], "brewfs-console");
        #[cfg(feature = "workspace-overlay")]
        assert!(dir.path().join("state/manager.sqlite").is_file());
    }

    #[cfg(feature = "workspace-overlay")]
    #[tokio::test]
    async fn manager_initialized_router_retains_single_instance_guard() {
        let dir = tempdir().unwrap();
        let first = crate::console::manager_api::initialized_router(dir.path().to_path_buf())
            .await
            .unwrap();
        let competing =
            crate::console::manager_api::initialized_router(dir.path().to_path_buf()).await;
        assert!(competing.is_err());
        drop(first);
        let reopened = crate::console::manager_api::initialized_router(dir.path().to_path_buf())
            .await
            .expect("dropping the router releases the Manager process guard");
        drop(reopened);
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
    async fn manager_capabilities_are_versioned_and_preserve_legacy_health() {
        let dir = tempdir().unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/capabilities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["service"], "brewfs-manager");
        assert_eq!(value["api_version"], "v1");
        assert_eq!(value["multi_replica"], false);
        assert_eq!(value["mount"], false);
        assert_eq!(value["remote_ready"], false);
        assert_eq!(value["enabled"], cfg!(feature = "workspace-overlay"));
        assert_eq!(value["max_fork_count"], 100);
        assert_eq!(value["max_pending_operations"], 128);
        assert_eq!(value["pending_limit_applies_to"], "new_operations");
        assert_eq!(value["max_concurrent_admissions"], 64);
        assert_eq!(value["batch_execution"], "serial");
        assert_eq!(value["batch_atomic"], false);
        assert_eq!(value["overload_http_status"], 429);
        assert_eq!(value["native_published_revision"], false);
        assert_eq!(value["native_frozen_metadata"], false);
        assert_eq!(value["workspace_entity"], false);
        assert_eq!(value["redis_cluster"], false);
        assert_eq!(value["redis_sentinel_discovery"], false);
        assert_eq!(value["benchmark"], false);
        assert_eq!(value["migration"], false);
        let enabled = cfg!(feature = "workspace-overlay");
        let native = cfg!(feature = "native-packed-base");
        assert_eq!(value["native_p1_catalog"], native);
        assert_eq!(
            value["metadata_backends"],
            serde_json::json!(if enabled {
                vec!["sqlite", "redis", "tikv"]
            } else {
                vec![]
            })
        );
        let formats = value["formats"].as_array().unwrap();
        assert_eq!(
            formats.contains(&serde_json::json!("workspace-v1")),
            enabled
        );
        assert_eq!(
            formats.contains(&serde_json::json!("workspace-native-v2")),
            native
        );
        let profiles = value["profiles"].as_array().unwrap();
        assert_eq!(profiles.len(), usize::from(enabled) + usize::from(native));
        if native {
            let profile = profiles
                .iter()
                .find(|profile| profile["volume_format"] == "workspace-native-v2")
                .unwrap();
            assert_eq!(
                profile["metadata_backends"],
                serde_json::json!(["redis", "tikv"])
            );
            assert_eq!(profile["revision_kind"], "native_p1_catalog");
            assert_eq!(profile["upper_bucket"], serde_json::Value::Null);
            assert_eq!(
                profile["object_storage"],
                "shared_pool_with_workspace_key_isolation"
            );
        }

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
    }

    #[tokio::test]
    async fn manager_mutations_require_existing_console_auth_before_opening_storage() {
        let dir = tempdir().unwrap();
        let app = build_router(test_config(
            dir.path(),
            AuthConfig::Token {
                token: "manager-secret".into(),
            },
        ));
        for uri in [
            "/api/v1/capabilities",
            "/api/v1/volumes",
            "/api/v1/operations/00000000-0000-0000-0000-000000000000",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(uri)
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, "Bearer wrong-secret")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(!body.contains("manager-secret"));
            assert!(!body.contains("wrong-secret"));
        }
        assert!(!dir.path().join("state").exists());
    }

    #[cfg(not(feature = "workspace-overlay"))]
    #[tokio::test]
    async fn manager_disabled_mutations_explicitly_report_unsupported() {
        let dir = tempdir().unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/volumes")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "unsupported");
    }

    #[cfg(feature = "workspace-overlay")]
    #[tokio::test]
    async fn manager_rejects_invalid_ids_keys_and_schema_without_opening_storage() {
        let dir = tempdir().unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));
        let requests = [
            Request::builder()
                .uri("/api/v1/volumes/not-a-uuid")
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method("POST")
                .uri("/api/v1/volumes")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
            Request::builder()
                .method("POST")
                .uri("/api/v1/volumes")
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "same-key")
                .header("Idempotency-Key", "another-key")
                .body(Body::from("{}"))
                .unwrap(),
            Request::builder()
                .method("POST")
                .uri("/api/v1/volumes")
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "bad-schema")
                .body(Body::from(
                    r#"{"mode":"initialize","catalog_ref":"local","password":"secret-marker"}"#,
                ))
                .unwrap(),
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/volumes/00000000-0000-0000-0000-000000000000/workspaces/00000000-0000-0000-0000-000000000000?force=true")
                .header("Idempotency-Key", "force-not-supported")
                .body(Body::empty()).unwrap(),
        ];
        for request in requests {
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["error"]["code"], "invalid_request");
            assert!(
                !String::from_utf8(body.to_vec())
                    .unwrap()
                    .contains("secret-marker")
            );
        }
        assert!(!dir.path().join("state").exists());
    }

    #[cfg(feature = "workspace-overlay")]
    #[tokio::test]
    async fn manager_rejects_invalid_pagination_before_opening_storage() {
        let dir = tempdir().unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));
        for query in ["limit=0", "limit=101", "after=not-a-uuid", "force=true"] {
            let response = app.clone().oneshot(
                Request::builder().uri(format!("/api/v1/volumes/00000000-0000-0000-0000-000000000000/workspaces?{query}"))
                    .body(Body::empty()).unwrap()
            ).await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        assert!(!dir.path().join("state").exists());
    }

    #[cfg(feature = "workspace-overlay")]
    #[tokio::test]
    async fn manager_rejects_native_revision_downgrades_and_connection_fields_before_storage() {
        let dir = tempdir().unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));
        let catalog_revision = serde_json::json!({
            "layer_id": uuid::Uuid::new_v4(), "sealed_version": 1, "root_hash": vec![0; 32]
        });
        let native = serde_json::json!({
            "kind": "native_p1_catalog", "volume_id": uuid::Uuid::new_v4(),
            "storage_namespace_id": vec![0; 16], "catalog_revision": catalog_revision
        });
        let mut published = native.clone();
        published["kind"] = "native_published".into();
        let mut missing_namespace = native.clone();
        missing_namespace
            .as_object_mut()
            .unwrap()
            .remove("storage_namespace_id");
        let mut nested_secret = native.clone();
        nested_secret["catalog_revision"]["endpoint"] =
            "redis://user:secret-marker@private-host/0".into();
        let mut legacy_secret = catalog_revision;
        legacy_secret["credential"] = "secret-marker".into();
        for revision in [published, missing_namespace, nested_secret, legacy_secret] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/v1/volumes/00000000-0000-0000-0000-000000000000/forks")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header("Idempotency-Key", "reject-invalid-native")
                        .body(Body::from(
                            serde_json::json!({"revision": revision, "count": 1}).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let text = String::from_utf8(body.to_vec()).unwrap();
            assert!(!text.contains("secret-marker"));
            assert!(!text.contains("private-host"));
        }
        for field in [
            "url",
            "url_env",
            "pd_endpoints",
            "namespace",
            "stable_cluster_id",
            "path",
            "upper_bucket",
        ] {
            let mut request = serde_json::json!({"mode":"initialize", "catalog_ref":"local", "confirm_empty":true});
            request[field] = "secret-marker".into();
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/v1/volumes")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header("Idempotency-Key", "reject-connection-field")
                        .body(Body::from(request.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{field}");
            let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            assert!(
                !String::from_utf8(body.to_vec())
                    .unwrap()
                    .contains("secret-marker")
            );
        }
        assert!(!dir.path().join("state").exists());
    }

    #[cfg(feature = "workspace-overlay")]
    async fn manager_json(
        app: &Router,
        method: &str,
        uri: &str,
        key: Option<&str>,
        body: Option<&serde_json::Value>,
    ) -> (StatusCode, serde_json::Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, "Bearer manager-test-token");
        if let Some(key) = key {
            request = request.header("Idempotency-Key", key);
        }
        let body = if let Some(body) = body {
            request = request.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(body).unwrap())
        } else {
            Body::empty()
        };
        let response = app
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let location = response.headers().get(header::LOCATION).cloned();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        if status == StatusCode::ACCEPTED {
            assert_eq!(
                location.as_ref().and_then(|value| value.to_str().ok()),
                Some(format!("/api/v1/operations/{}", value["id"].as_str().unwrap()).as_str())
            );
        }
        (status, value)
    }

    #[cfg(feature = "workspace-overlay")]
    async fn manager_finished(app: &Router, id: &serde_json::Value) -> serde_json::Value {
        let uri = format!("/api/v1/operations/{}", id.as_str().unwrap());
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let (status, operation) = manager_json(app, "GET", &uri, None, None).await;
                assert_eq!(status, StatusCode::OK, "{operation}");
                match operation["state"].as_str() {
                    Some("succeeded") => return operation,
                    Some("accepted" | "running") => {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    _ => panic!("unexpected Manager operation state: {operation}"),
                }
            }
        })
        .await
        .expect("Manager operation timed out")
    }

    #[cfg(feature = "workspace-overlay")]
    #[tokio::test]
    async fn manager_operation_lookup_starts_without_resolving_missing_remote_credentials() {
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join("state");
        std::fs::create_dir(&state_dir).unwrap();
        let variable = format!("BREWFS_MANAGER_MISSING_{}", uuid::Uuid::new_v4().simple());
        std::fs::write(
            state_dir.join("manager-catalogs.json"),
            serde_json::json!({"catalogs": {"remote": {
                "backend": "redis", "url_env": variable,
                "namespace": "offline-catalog", "stable_cluster_id": "offline-cluster"
            }}})
            .to_string(),
        )
        .unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));
        let (status, response) = manager_json(
            &app,
            "GET",
            "/api/v1/operations/00000000-0000-0000-0000-000000000000",
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{response}");
        assert_eq!(response["error"]["code"], "not_found");
        assert!(!response.to_string().contains(&variable));
        assert!(state_dir.join("manager.sqlite").is_file());
    }

    #[cfg(feature = "workspace-overlay")]
    #[tokio::test]
    async fn manager_http_rejects_native_sqlite_before_creating_catalog_metadata() {
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join("state");
        std::fs::create_dir(&state_dir).unwrap();
        let catalog_path = dir.path().join("must-not-be-created.sqlite");
        std::fs::write(
            state_dir.join("manager-catalogs.json"),
            serde_json::json!({
                "catalogs":{"local":{"backend":"sqlite", "path":catalog_path}}
            })
            .to_string(),
        )
        .unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));
        let (status, response) = manager_json(&app, "POST", "/api/v1/volumes", Some("unsupported-native-sqlite"), Some(&serde_json::json!({
            "mode":"initialize", "catalog_ref":"local", "confirm_empty":true, "volume_format":"workspace-native-v2"
        }))).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{response}");
        assert_eq!(response["error"]["code"], "unsupported");
        assert!(!catalog_path.exists());
        let (status, response) = manager_json(&app, "GET", "/api/v1/volumes", None, None).await;
        assert_eq!(status, StatusCode::OK, "{response}");
        assert_eq!(response["volumes"], serde_json::json!([]));
    }

    #[cfg(feature = "workspace-overlay")]
    #[tokio::test]
    async fn manager_http_uses_real_catalog_and_scopes_forks_and_deletion() {
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join("state");
        std::fs::create_dir(&state_dir).unwrap();
        std::fs::write(
            state_dir.join("manager-catalogs.json"),
            serde_json::to_vec(&serde_json::json!({"catalogs": {"local": {
                "backend": "sqlite", "path": dir.path().join("catalog.sqlite")
            }}}))
            .unwrap(),
        )
        .unwrap();
        let app = build_router(test_config(
            dir.path(),
            AuthConfig::Token {
                token: "manager-test-token".into(),
            },
        ));
        let create = serde_json::json!({
            "mode": "initialize", "catalog_ref": "local", "confirm_empty": true,
            "owner": "root-owner"
        });
        let (status, accepted) = manager_json(
            &app,
            "POST",
            "/api/v1/volumes",
            Some("initialize-one"),
            Some(&create),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
        let (status, retried) = manager_json(
            &app,
            "POST",
            "/api/v1/volumes",
            Some("initialize-one"),
            Some(&create),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{retried}");
        assert_eq!(accepted["id"], retried["id"]);
        let initialized = manager_finished(&app, &accepted["id"]).await;
        assert!(initialized["items"][0]["remote_ready"].is_null());
        assert_eq!(initialized["items"][0]["metadata_ready"], true);
        let volume = initialized["volume_id"].as_str().unwrap();
        let workspaces_uri = format!("/api/v1/volumes/{volume}/workspaces");
        let (status, listed) = manager_json(&app, "GET", &workspaces_uri, None, None).await;
        assert_eq!(status, StatusCode::OK, "{listed}");
        let roots = listed["workspaces"].as_array().unwrap();
        assert_eq!(roots.len(), 1);
        let root = roots[0]["workspace_id"].as_str().unwrap();
        let (_, inspected) =
            manager_json(&app, "GET", &format!("{workspaces_uri}/{root}"), None, None).await;
        assert!(inspected.get("record").is_some());
        assert!(inspected.get("layer_depth").is_some());
        assert!(inspected["leases"].is_array());
        assert!(inspected.get("native").is_none());
        assert_eq!(inspected["mount_identity"]["catalog_ref"], "local");
        assert_eq!(inspected["mount_identity"]["metadata_backend"], "sqlite");
        assert_eq!(inspected["mount_identity"]["volume_format"], "workspace-v1");
        assert_eq!(inspected["mount_identity"]["volume_id"], volume);
        assert_eq!(inspected["mount_identity"]["workspace_id"], root);
        assert!(inspected["mount_identity"]["upper_bucket"].is_null());
        assert!(inspected["mount_identity"].get("path").is_none());
        assert!(inspected["mount_identity"].get("url").is_none());
        let revision_uri = format!(
            "/api/v1/volumes/{volume}/revisions/{}",
            inspected["base_revision"]["layer_id"].as_str().unwrap()
        );
        let (status, revision) = manager_json(&app, "GET", &revision_uri, None, None).await;
        assert_eq!(status, StatusCode::OK, "{revision}");
        assert_eq!(revision, inspected["base_revision"]);
        let forks_uri = format!("/api/v1/volumes/{volume}/forks");
        let forks = serde_json::json!({"revision": inspected["base_revision"], "count": 2, "owner": "fork-owner"});
        for count in [0, 101, 128] {
            let mut excessive = forks.clone();
            excessive["count"] = count.into();
            let (status, rejected) = manager_json(
                &app,
                "POST",
                &forks_uri,
                Some("invalid-count"),
                Some(&excessive),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
        }
        let (status, accepted) =
            manager_json(&app, "POST", &forks_uri, Some("fork-two"), Some(&forks)).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{accepted}");
        let (status, retried) =
            manager_json(&app, "POST", &forks_uri, Some("fork-two"), Some(&forks)).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{retried}");
        assert_eq!(accepted["id"], retried["id"]);
        manager_finished(&app, &accepted["id"]).await;
        let mut changed = forks.clone();
        changed["owner"] = serde_json::json!("other-owner");
        let (status, conflict) =
            manager_json(&app, "POST", &forks_uri, Some("fork-two"), Some(&changed)).await;
        assert_eq!(status, StatusCode::CONFLICT, "{conflict}");

        let (status, listed) = manager_json(
            &app,
            "GET",
            &format!("{workspaces_uri}?limit=2"),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{listed}");
        assert_eq!(listed["workspaces"].as_array().unwrap().len(), 2);
        let after = listed["next_after"].as_str().unwrap();
        let (status, second_page) = manager_json(
            &app,
            "GET",
            &format!("{workspaces_uri}?limit=2&after={after}"),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{second_page}");
        assert_eq!(second_page["workspaces"].as_array().unwrap().len(), 1);

        let (_, listed) = manager_json(&app, "GET", &workspaces_uri, None, None).await;
        let child = listed["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .find(|record| record["owner_id"] == "fork-owner")
            .unwrap()["workspace_id"]
            .as_str()
            .unwrap();
        let (status, foreign) = manager_json(
            &app,
            "GET",
            &format!(
                "/api/v1/volumes/{}/workspaces/{child}",
                uuid::Uuid::new_v4()
            ),
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{foreign}");
        let child_uri = format!("{workspaces_uri}/{child}");
        let (status, deletion) =
            manager_json(&app, "DELETE", &child_uri, Some("delete-child"), None).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{deletion}");
        let deleted = manager_finished(&app, &deletion["id"]).await;
        assert_eq!(deleted["items"][0]["deletion_marked"], true);
        assert_eq!(deleted["items"][0]["physical_reclaimed"], false);
        let (status, repeat) =
            manager_json(&app, "DELETE", &child_uri, Some("delete-child"), None).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{repeat}");
        assert_eq!(deletion["id"], repeat["id"]);
        assert!(dir.path().join("catalog.sqlite").exists());

        let (status, legacy) = manager_json(&app, "GET", "/api/volumes", None, None).await;
        assert_eq!(status, StatusCode::OK, "{legacy}");
        assert_eq!(legacy["volumes"].as_array().unwrap().len(), 0);
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
    async fn volumes_api_marks_registered_filesystems_with_live_runtime_state() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let pid = std::process::id();
        let socket_path = registry.socket_path(pid);
        let record = crate::control::runtime::InstanceRecord::new(
            pid,
            "/mnt/brewfs".to_string(),
            socket_path,
            chrono::Utc::now(),
        );
        registry.write_record(&record).await.unwrap();
        let app = build_router(config);

        for (name, mount_point) in [("mounted", "/mnt/brewfs"), ("offline", "/mnt/offline")] {
            let create_body = serde_json::json!({
                "name": name,
                "mount_config": {
                    "mount_point": mount_point,
                    "data_backend": "local-fs",
                    "meta_backend": "sqlx"
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
        }

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
        let mounted = listed["volumes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|volume| volume["name"] == "mounted")
            .unwrap();
        let offline = listed["volumes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|volume| volume["name"] == "offline")
            .unwrap();

        assert_eq!(mounted["runtime"]["mounted"], true);
        assert_eq!(mounted["runtime"]["pid"], pid);
        assert_eq!(offline["runtime"]["mounted"], false);
        assert_eq!(offline["runtime"]["pid"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn volumes_api_gets_updates_and_deletes_registry_entries() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));

        let create_body = serde_json::json!({
            "name": "dev-local",
            "mount_config": {
                "data_backend": "local-fs",
                "meta_backend": "sqlx"
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
        let volume_id = created["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/volumes/{volume_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let patch_body = serde_json::json!({
            "name": "prod-local",
            "description": null,
            "labels": { "env": "prod" }
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/volumes/{volume_id}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(patch_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let updated: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(updated["name"], "prod-local");
        assert_eq!(updated["labels"]["env"], "prod");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/volumes/{volume_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/volumes/{volume_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
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

    #[tokio::test]
    async fn instance_detail_rejects_control_plane_pid_mismatch() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let pid = std::process::id();
        let socket_path = registry.socket_path(pid);
        let _server = crate::control::server::ControlServer::bind(
            socket_path.clone(),
            MismatchedInfoHandler { pid: pid + 1 },
        )
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

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "control_plane_error");
    }

    #[tokio::test]
    async fn instance_gc_job_returns_accepted_job_id() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let pid = std::process::id();
        let socket_path = registry.socket_path(pid);
        let _server =
            crate::control::server::ControlServer::bind(socket_path.clone(), GcJobHandler)
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
                    .method("POST")
                    .uri(format!("/api/instances/{pid}/jobs/gc"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"dry_run":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["job_id"], "job-gc-1");
    }

    #[tokio::test]
    async fn instance_job_status_calls_control_plane_get_job() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let pid = std::process::id();
        let socket_path = registry.socket_path(pid);
        let _server =
            crate::control::server::ControlServer::bind(socket_path.clone(), GcJobHandler)
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
                    .uri(format!("/api/instances/{pid}/jobs/job-gc-1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["job_id"], "job-gc-1");
        assert_eq!(value["state"], "Succeeded");
        assert_eq!(value["outcome"]["Gc"]["dry_run"], true);
    }

    #[tokio::test]
    async fn instance_job_status_rejects_control_plane_job_id_mismatch() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let pid = std::process::id();
        let socket_path = registry.socket_path(pid);
        let _server =
            crate::control::server::ControlServer::bind(socket_path.clone(), MismatchedJobHandler)
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
                    .uri(format!("/api/instances/{pid}/jobs/job-gc-1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "control_plane_error");
    }

    #[tokio::test]
    async fn csi_summary_returns_kubernetes_error_when_kubeconfig_cannot_load() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let mut config = test_config(dir.path(), AuthConfig::Disabled);
        config.csi.enabled = true;
        config.csi.kubeconfig = Some(dir.path().join("missing-kubeconfig"));
        let app = build_router(config);

        let requests = [Request::builder()
            .uri("/api/csi/summary")
            .body(Body::empty())
            .unwrap()];

        for request in requests {
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                "application/json"
            );
            let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["error"]["code"], "kubernetes_error");
        }
    }

    #[tokio::test]
    async fn csi_summary_returns_unavailable_when_dashboard_is_disabled() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/csi/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "unavailable");
    }

    #[tokio::test]
    async fn csi_resource_routes_are_unavailable_when_dashboard_is_disabled() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));

        for uri in [
            "/api/csi/storageclasses",
            "/api/csi/persistentvolumes",
            "/api/csi/persistentvolumeclaims?namespace=default",
            "/api/csi/pods?namespace=default&volume=data",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::CONFLICT);
            let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["error"]["code"], "unavailable");
        }
    }

    #[tokio::test]
    async fn csi_resource_routes_return_kubernetes_error_when_kubeconfig_cannot_load() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let mut config = test_config(dir.path(), AuthConfig::Disabled);
        config.csi.enabled = true;
        config.csi.kubeconfig = Some(dir.path().join("missing-kubeconfig"));
        let app = build_router(config);

        for uri in [
            "/api/csi/storageclasses",
            "/api/csi/persistentvolumes",
            "/api/csi/persistentvolumeclaims?namespace=default",
            "/api/csi/pods?namespace=default&volume=data",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["error"]["code"], "kubernetes_error");
        }
    }

    #[tokio::test]
    async fn trash_api_reports_unsupported_when_volume_is_mounted() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let pid = std::process::id();
        let socket_path = registry.socket_path(pid);
        let _server = crate::control::server::ControlServer::bind(
            socket_path.clone(),
            UnsupportedTrashHandler,
        )
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
        let volume_id = create_live_browser_volume(&app).await;

        for request in [
            Request::builder()
                .uri(format!("/api/volumes/{volume_id}/trash"))
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method("POST")
                .uri(format!("/api/volumes/{volume_id}/trash/trash-1/restore"))
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/volumes/{volume_id}/trash/trash-1"))
                .body(Body::empty())
                .unwrap(),
        ] {
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["error"]["code"], "unsupported");
        }
    }

    #[tokio::test]
    async fn trash_api_returns_instance_unavailable_when_registered_volume_is_not_mounted() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));
        let volume_id = create_live_browser_volume(&app).await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/volumes/{volume_id}/trash"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "instance_unavailable");
    }

    #[tokio::test]
    async fn trash_api_uses_default_control_plane_adapter_when_volume_is_mounted() {
        let dir = tempdir().unwrap();
        let (app, _server) = build_mounted_app_with_handler(dir.path(), ReadyFeatureHandler).await;
        let volume_id = create_live_browser_volume(&app).await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/volumes/{volume_id}/trash"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["entries"][0]["id"], "trash-1");

        for request in [
            Request::builder()
                .method("POST")
                .uri(format!("/api/volumes/{volume_id}/trash/trash-1/restore"))
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/volumes/{volume_id}/trash/trash-1"))
                .body(Body::empty())
                .unwrap(),
        ] {
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["ok"], true);
        }
    }

    #[tokio::test]
    async fn acl_api_returns_instance_unavailable_when_registered_volume_is_not_mounted() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));
        let volume_id = create_live_browser_volume(&app).await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/volumes/{volume_id}/acl?path=/"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "instance_unavailable");
    }

    #[tokio::test]
    async fn acl_api_reports_backend_without_acl_capability() {
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
        let volume_id = create_live_browser_volume(&app).await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/volumes/{volume_id}/acl?path=/"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "unsupported");
    }

    #[tokio::test]
    async fn acl_api_uses_default_control_plane_adapter_when_capability_is_enabled() {
        let dir = tempdir().unwrap();
        let (app, _server) = build_mounted_app_with_handler(dir.path(), ReadyFeatureHandler).await;
        let volume_id = create_live_browser_volume(&app).await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/volumes/{volume_id}/acl?path=/docs"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["entries"][0]["tag"], "user_obj");

        let acl_body = serde_json::json!({
            "entries": [
                {
                    "scope": "access",
                    "tag": "user_obj",
                    "perm": "rwx"
                },
                {
                    "scope": "access",
                    "tag": "group_obj",
                    "perm": "r-x"
                },
                {
                    "scope": "access",
                    "tag": "other",
                    "perm": "---"
                },
                {
                    "scope": "access",
                    "tag": "group",
                    "id": 1000,
                    "perm": "r-x"
                }
            ]
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/api/volumes/{volume_id}/acl?path=/docs"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&acl_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["entries"][3]["tag"], "group");

        let invalid_acl_body = serde_json::json!({
            "entries": [{
                "scope": "access",
                "tag": "group_obj",
                "perm": "read"
            }]
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/api/volumes/{volume_id}/acl?path=/docs"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&invalid_acl_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "invalid_request");

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/volumes/{volume_id}/acl?path=/docs"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["ok"], true);
    }

    #[tokio::test]
    async fn files_api_returns_instance_unavailable_when_registered_volume_is_not_mounted() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));

        let create_body = serde_json::json!({
            "name": "dev-local",
            "mount_config": {
                "mount_point": "/mnt/brewfs",
                "data_backend": "local-fs",
                "meta_backend": "sqlx"
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
        let volume_id = created["id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/volumes/{volume_id}/files?path=/"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "instance_unavailable");
    }

    #[tokio::test]
    async fn files_api_rejects_relative_paths() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let app = build_router(test_config(dir.path(), AuthConfig::Disabled));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/volumes/vol-1/files?path=relative")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "invalid_path");
    }

    #[tokio::test]
    async fn files_api_lists_entries_from_live_control_plane() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let pid = std::process::id();
        let socket_path = registry.socket_path(pid);
        let _server =
            crate::control::server::ControlServer::bind(socket_path.clone(), FileListHandler)
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

        let create_body = serde_json::json!({
            "name": "dev-local",
            "mount_config": {
                "mount_point": "/mnt/brewfs",
                "data_backend": "local-fs",
                "meta_backend": "sqlx"
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
        let volume_id = created["id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/volumes/{volume_id}/files?path=/projects/../projects"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["path"], "/projects");
        assert_eq!(value["entries"][0]["name"], "notes.txt");
        assert_eq!(value["entries"][0]["kind"], "file");
        assert_eq!(value["entries"][0]["inode"], 42);
        assert_eq!(value["entries"][0]["size"], 128);
        assert_eq!(value["entries"][0]["mode"], 0o644);
        assert_eq!(value["entries"][0]["has_acl"], true);
    }

    #[tokio::test]
    async fn files_api_rejects_control_plane_path_mismatch() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let pid = std::process::id();
        let socket_path = registry.socket_path(pid);
        let _server = crate::control::server::ControlServer::bind(
            socket_path.clone(),
            MismatchedDirectoryHandler,
        )
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

        let create_body = serde_json::json!({
            "name": "dev-local",
            "mount_config": {
                "mount_point": "/mnt/brewfs",
                "data_backend": "local-fs",
                "meta_backend": "sqlx"
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
        let volume_id = created["id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/volumes/{volume_id}/files?path=/projects"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], "control_plane_error");
    }

    #[tokio::test]
    async fn file_stat_api_returns_metadata_from_live_control_plane() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let pid = std::process::id();
        let socket_path = registry.socket_path(pid);
        let _server =
            crate::control::server::ControlServer::bind(socket_path.clone(), FileMetadataHandler)
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

        let volume_id = create_live_browser_volume(&app).await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/volumes/{volume_id}/files/stat?path=/projects/readme.md"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["path"], "/projects/readme.md");
        assert_eq!(value["kind"], "file");
        assert_eq!(value["inode"], 42);
        assert_eq!(value["size"], 128);
        assert_eq!(value["mode"], 0o644);
    }

    #[tokio::test]
    async fn file_readlink_api_returns_target_from_live_control_plane() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("index.html"), "<div id=\"root\"></div>").unwrap();
        let config = test_config(dir.path(), AuthConfig::Disabled);
        let registry = crate::control::runtime::RuntimeRegistry::new(config.runtime_dir.clone());
        let pid = std::process::id();
        let socket_path = registry.socket_path(pid);
        let _server =
            crate::control::server::ControlServer::bind(socket_path.clone(), FileMetadataHandler)
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

        let volume_id = create_live_browser_volume(&app).await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/volumes/{volume_id}/files/readlink?path=/latest"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["path"], "/latest");
        assert_eq!(value["target"], "/projects/readme.md");
    }

    async fn create_live_browser_volume(app: &Router) -> String {
        let create_body = serde_json::json!({
            "name": "dev-local",
            "mount_config": {
                "mount_point": "/mnt/brewfs",
                "data_backend": "local-fs",
                "meta_backend": "sqlx"
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
        created["id"].as_str().unwrap().to_string()
    }

    struct ReadyFeatureHandler;

    #[async_trait::async_trait]
    impl crate::control::server::ControlHandler for ReadyFeatureHandler {
        async fn handle(
            &self,
            request: crate::control::protocol::ControlRequest,
        ) -> crate::control::protocol::ControlResponse {
            match request {
                crate::control::protocol::ControlRequest::GetInfo => {
                    let capabilities = crate::meta::store::MetaStoreCapabilities {
                        namespace: true,
                        file_data: true,
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
                crate::control::protocol::ControlRequest::GetAcl { path } => {
                    crate::control::protocol::ControlResponse::Acl {
                        path,
                        entries: vec![crate::control::protocol::ControlAclEntry {
                            scope: "access".to_string(),
                            tag: "user_obj".to_string(),
                            id: None,
                            perm: "rwx".to_string(),
                        }],
                    }
                }
                crate::control::protocol::ControlRequest::PutAcl { path, entries } => {
                    crate::control::protocol::ControlResponse::Acl { path, entries }
                }
                crate::control::protocol::ControlRequest::DeleteAcl { path } => {
                    crate::control::protocol::ControlResponse::AclDeleted { path }
                }
                crate::control::protocol::ControlRequest::ListTrash => {
                    crate::control::protocol::ControlResponse::Trash {
                        entries: vec![crate::control::protocol::ControlTrashEntry {
                            id: "trash-1".to_string(),
                            original_path: "/docs/report.txt".to_string(),
                            size: Some(42),
                            deleted_at: Some("2026-06-11T12:00:00Z".to_string()),
                        }],
                    }
                }
                crate::control::protocol::ControlRequest::RestoreTrashEntry { entry_id } => {
                    crate::control::protocol::ControlResponse::TrashRestored { entry_id }
                }
                crate::control::protocol::ControlRequest::DeleteTrashEntry { entry_id } => {
                    crate::control::protocol::ControlResponse::TrashDeleted { entry_id }
                }
                other => crate::control::protocol::ControlResponse::Error {
                    code: "unexpected".to_string(),
                    message: format!("unexpected request: {other:?}"),
                },
            }
        }
    }

    struct UnsupportedTrashHandler;

    #[async_trait::async_trait]
    impl crate::control::server::ControlHandler for UnsupportedTrashHandler {
        async fn handle(
            &self,
            request: crate::control::protocol::ControlRequest,
        ) -> crate::control::protocol::ControlResponse {
            match request {
                crate::control::protocol::ControlRequest::ListTrash
                | crate::control::protocol::ControlRequest::RestoreTrashEntry { .. }
                | crate::control::protocol::ControlRequest::DeleteTrashEntry { .. } => {
                    crate::control::protocol::ControlResponse::Error {
                        code: "unsupported".to_string(),
                        message: "trash control-plane requests are not implemented yet".to_string(),
                    }
                }
                other => crate::control::protocol::ControlResponse::Error {
                    code: "unexpected".to_string(),
                    message: format!("unexpected request: {other:?}"),
                },
            }
        }
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

    struct MismatchedInfoHandler {
        pid: u32,
    }

    #[async_trait::async_trait]
    impl crate::control::server::ControlHandler for MismatchedInfoHandler {
        async fn handle(
            &self,
            request: crate::control::protocol::ControlRequest,
        ) -> crate::control::protocol::ControlResponse {
            match request {
                crate::control::protocol::ControlRequest::GetInfo => {
                    crate::control::protocol::ControlResponse::Info {
                        pid: self.pid,
                        mount_point: "/mnt/brewfs-other".to_string(),
                        started_at: 1_786_000_000_000,
                        version: "0.1.0-test".to_string(),
                        meta_backend: "sqlx".to_string(),
                        capabilities: Default::default(),
                    }
                }
                other => crate::control::protocol::ControlResponse::Error {
                    code: "unexpected".to_string(),
                    message: format!("unexpected request: {other:?}"),
                },
            }
        }
    }

    struct GcJobHandler;

    #[async_trait::async_trait]
    impl crate::control::server::ControlHandler for GcJobHandler {
        async fn handle(
            &self,
            request: crate::control::protocol::ControlRequest,
        ) -> crate::control::protocol::ControlResponse {
            match request {
                crate::control::protocol::ControlRequest::RunGc { dry_run: true } => {
                    crate::control::protocol::ControlResponse::Accepted {
                        job_id: "job-gc-1".to_string(),
                    }
                }
                crate::control::protocol::ControlRequest::GetJob { job_id }
                    if job_id == "job-gc-1" =>
                {
                    crate::control::protocol::ControlResponse::JobStatus {
                        job_id,
                        state: crate::control::job::JobState::Succeeded,
                        detail: Some("gc complete".to_string()),
                        outcome: Some(crate::control::job::JobOutcome::Gc(
                            crate::control::job::GcJobResult {
                                dry_run: true,
                                orphan_slice_count: 3,
                                orphan_object_count: 2,
                                deleted_object_count: 0,
                                error_count: 0,
                                detail: Some("gc complete".to_string()),
                            },
                        )),
                    }
                }
                other => crate::control::protocol::ControlResponse::Error {
                    code: "unexpected".to_string(),
                    message: format!("unexpected request: {other:?}"),
                },
            }
        }
    }

    struct MismatchedJobHandler;

    #[async_trait::async_trait]
    impl crate::control::server::ControlHandler for MismatchedJobHandler {
        async fn handle(
            &self,
            request: crate::control::protocol::ControlRequest,
        ) -> crate::control::protocol::ControlResponse {
            match request {
                crate::control::protocol::ControlRequest::GetJob { .. } => {
                    crate::control::protocol::ControlResponse::JobStatus {
                        job_id: "different-job".to_string(),
                        state: crate::control::job::JobState::Succeeded,
                        detail: None,
                        outcome: None,
                    }
                }
                other => crate::control::protocol::ControlResponse::Error {
                    code: "unexpected".to_string(),
                    message: format!("unexpected request: {other:?}"),
                },
            }
        }
    }

    struct FileListHandler;

    #[async_trait::async_trait]
    impl crate::control::server::ControlHandler for FileListHandler {
        async fn handle(
            &self,
            request: crate::control::protocol::ControlRequest,
        ) -> crate::control::protocol::ControlResponse {
            match request {
                crate::control::protocol::ControlRequest::ListDirectory { path }
                    if path == "/projects" =>
                {
                    crate::control::protocol::ControlResponse::DirectoryListing {
                        path,
                        entries: vec![crate::control::protocol::ControlDirectoryEntry {
                            name: "notes.txt".to_string(),
                            inode: 42,
                            kind: crate::control::protocol::ControlFileKind::File,
                            size: 128,
                            mode: 0o644,
                            uid: 1000,
                            gid: 1000,
                            mtime_ns: 1_786_000_000_000_000_000,
                            has_acl: true,
                        }],
                    }
                }
                other => crate::control::protocol::ControlResponse::Error {
                    code: "unexpected".to_string(),
                    message: format!("unexpected request: {other:?}"),
                },
            }
        }
    }

    struct MismatchedDirectoryHandler;

    #[async_trait::async_trait]
    impl crate::control::server::ControlHandler for MismatchedDirectoryHandler {
        async fn handle(
            &self,
            request: crate::control::protocol::ControlRequest,
        ) -> crate::control::protocol::ControlResponse {
            match request {
                crate::control::protocol::ControlRequest::ListDirectory { .. } => {
                    crate::control::protocol::ControlResponse::DirectoryListing {
                        path: "/other".to_string(),
                        entries: Vec::new(),
                    }
                }
                other => crate::control::protocol::ControlResponse::Error {
                    code: "unexpected".to_string(),
                    message: format!("unexpected request: {other:?}"),
                },
            }
        }
    }

    struct FileMetadataHandler;

    #[async_trait::async_trait]
    impl crate::control::server::ControlHandler for FileMetadataHandler {
        async fn handle(
            &self,
            request: crate::control::protocol::ControlRequest,
        ) -> crate::control::protocol::ControlResponse {
            match request {
                crate::control::protocol::ControlRequest::StatPath { path }
                    if path == "/projects/readme.md" =>
                {
                    crate::control::protocol::ControlResponse::PathMetadata {
                        path,
                        metadata: crate::control::protocol::ControlPathMetadata {
                            inode: 42,
                            kind: crate::control::protocol::ControlFileKind::File,
                            size: 128,
                            mode: 0o644,
                            uid: 1000,
                            gid: 1000,
                            mtime_ns: 1_786_000_000_000_000_000,
                        },
                    }
                }
                crate::control::protocol::ControlRequest::ReadLink { path }
                    if path == "/latest" =>
                {
                    crate::control::protocol::ControlResponse::SymlinkTarget {
                        path,
                        target: "/projects/readme.md".to_string(),
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
