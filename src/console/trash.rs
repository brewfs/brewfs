use crate::control::{
    client::send_request,
    protocol::{ControlRequest, ControlResponse, ControlTrashEntry},
    runtime::InstanceRecord,
};
use async_trait::async_trait;
use serde::Serialize;
use std::{fmt, sync::Arc, time::Duration};

const TRASH_CONTROL_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TrashEntry {
    pub id: String,
    pub original_path: String,
    pub size: Option<u64>,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TrashList {
    pub entries: Vec<TrashEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TrashActionResponse {
    pub ok: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrashAdapterError {
    Unsupported(String),
    ControlPlane(String),
}

#[async_trait]
pub trait TrashAdapter: fmt::Debug + Send + Sync {
    async fn list(
        &self,
        volume_id: &str,
        runtime: &InstanceRecord,
    ) -> Result<TrashList, TrashAdapterError>;

    async fn restore(
        &self,
        volume_id: &str,
        entry_id: &str,
        runtime: &InstanceRecord,
    ) -> Result<(), TrashAdapterError>;

    async fn delete(
        &self,
        volume_id: &str,
        entry_id: &str,
        runtime: &InstanceRecord,
    ) -> Result<(), TrashAdapterError>;
}

pub fn default_trash_adapter() -> Arc<dyn TrashAdapter> {
    Arc::new(ControlPlaneTrashAdapter)
}

#[derive(Debug)]
struct ControlPlaneTrashAdapter;

#[async_trait]
impl TrashAdapter for ControlPlaneTrashAdapter {
    async fn list(
        &self,
        _volume_id: &str,
        runtime: &InstanceRecord,
    ) -> Result<TrashList, TrashAdapterError> {
        match send_trash_request(runtime, ControlRequest::ListTrash).await? {
            ControlResponse::Trash { entries } => Ok(TrashList {
                entries: entries.into_iter().map(TrashEntry::from).collect(),
            }),
            ControlResponse::Error { code, message } => control_error(code, message),
            other => unexpected_response(other),
        }
    }

    async fn restore(
        &self,
        _volume_id: &str,
        entry_id: &str,
        runtime: &InstanceRecord,
    ) -> Result<(), TrashAdapterError> {
        match send_trash_request(
            runtime,
            ControlRequest::RestoreTrashEntry {
                entry_id: entry_id.to_string(),
            },
        )
        .await?
        {
            ControlResponse::TrashRestored {
                entry_id: response_entry_id,
            } if response_entry_id == entry_id => Ok(()),
            ControlResponse::TrashRestored {
                entry_id: response_entry_id,
            } => Err(TrashAdapterError::ControlPlane(format!(
                "trash entry mismatch: requested {entry_id}, got {response_entry_id}",
            ))),
            ControlResponse::Error { code, message } => control_error(code, message),
            other => unexpected_response(other),
        }
    }

    async fn delete(
        &self,
        _volume_id: &str,
        entry_id: &str,
        runtime: &InstanceRecord,
    ) -> Result<(), TrashAdapterError> {
        match send_trash_request(
            runtime,
            ControlRequest::DeleteTrashEntry {
                entry_id: entry_id.to_string(),
            },
        )
        .await?
        {
            ControlResponse::TrashDeleted {
                entry_id: response_entry_id,
            } if response_entry_id == entry_id => Ok(()),
            ControlResponse::TrashDeleted {
                entry_id: response_entry_id,
            } => Err(TrashAdapterError::ControlPlane(format!(
                "trash entry mismatch: requested {entry_id}, got {response_entry_id}",
            ))),
            ControlResponse::Error { code, message } => control_error(code, message),
            other => unexpected_response(other),
        }
    }
}

async fn send_trash_request(
    runtime: &InstanceRecord,
    request: ControlRequest,
) -> Result<ControlResponse, TrashAdapterError> {
    send_trash_request_with_timeout(runtime, request, TRASH_CONTROL_TIMEOUT).await
}

async fn send_trash_request_with_timeout(
    runtime: &InstanceRecord,
    request: ControlRequest,
    timeout: Duration,
) -> Result<ControlResponse, TrashAdapterError> {
    tokio::time::timeout(timeout, send_request(&runtime.socket_path, &request))
        .await
        .map_err(|_| {
            TrashAdapterError::ControlPlane("trash control-plane request timed out".to_string())
        })?
        .map_err(|err| {
            TrashAdapterError::ControlPlane(format!("trash control-plane request failed: {err}"))
        })
}

fn control_error<T>(code: String, message: String) -> Result<T, TrashAdapterError> {
    if code == "unsupported" {
        Err(TrashAdapterError::Unsupported(message))
    } else {
        Err(TrashAdapterError::ControlPlane(format!(
            "{code}: {message}"
        )))
    }
}

fn unexpected_response<T>(response: ControlResponse) -> Result<T, TrashAdapterError> {
    Err(TrashAdapterError::ControlPlane(format!(
        "unexpected trash control-plane response: {response:?}",
    )))
}

impl From<ControlTrashEntry> for TrashEntry {
    fn from(entry: ControlTrashEntry) -> Self {
        Self {
            id: entry.id,
            original_path: entry.original_path,
            size: entry.size,
            deleted_at: entry.deleted_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::protocol::{ControlRequest, ControlResponse, ControlTrashEntry};
    use crate::control::runtime::InstanceRecord;
    use crate::control::server::{ControlHandler, ControlServer};

    #[tokio::test]
    async fn default_adapter_forwards_trash_requests_to_control_plane() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("control.sock");
        let _server = ControlServer::bind(socket_path.clone(), TrashHandler)
            .await
            .unwrap();
        let runtime = InstanceRecord::new(
            std::process::id(),
            "/mnt/brewfs".to_string(),
            socket_path,
            chrono::Utc::now(),
        );
        let adapter = default_trash_adapter();

        let list = adapter.list("vol-1", &runtime).await.unwrap();
        assert_eq!(list.entries[0].id, "trash-1");
        assert_eq!(list.entries[0].original_path, "/docs/report.txt");

        adapter.restore("vol-1", "trash-1", &runtime).await.unwrap();
        adapter.delete("vol-1", "trash-1", &runtime).await.unwrap();
    }

    #[tokio::test]
    async fn trash_request_times_out_when_control_plane_stalls() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("control.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let _task = tokio::spawn(async move {
            let Ok((_stream, _addr)) = listener.accept().await else {
                return;
            };
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        let runtime = InstanceRecord::new(
            std::process::id(),
            "/mnt/brewfs".to_string(),
            socket_path,
            chrono::Utc::now(),
        );

        let err = send_trash_request_with_timeout(
            &runtime,
            ControlRequest::ListTrash,
            std::time::Duration::from_millis(10),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            TrashAdapterError::ControlPlane(message) if message.contains("timed out")
        ));
    }

    struct TrashHandler;

    #[async_trait]
    impl ControlHandler for TrashHandler {
        async fn handle(&self, request: ControlRequest) -> ControlResponse {
            match request {
                ControlRequest::ListTrash => ControlResponse::Trash {
                    entries: vec![ControlTrashEntry {
                        id: "trash-1".to_string(),
                        original_path: "/docs/report.txt".to_string(),
                        size: Some(42),
                        deleted_at: Some("2026-06-11T12:00:00Z".to_string()),
                    }],
                },
                ControlRequest::RestoreTrashEntry { entry_id } => {
                    ControlResponse::TrashRestored { entry_id }
                }
                ControlRequest::DeleteTrashEntry { entry_id } => {
                    ControlResponse::TrashDeleted { entry_id }
                }
                other => ControlResponse::Error {
                    code: "unexpected".to_string(),
                    message: format!("unexpected request: {other:?}"),
                },
            }
        }
    }
}
