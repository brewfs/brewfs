use crate::control::{
    client::send_request,
    protocol::{ControlAclEntry, ControlRequest, ControlResponse, validate_acl_entries},
    runtime::InstanceRecord,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc, time::Duration};

const ACL_CONTROL_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclEntry {
    pub scope: String,
    pub tag: String,
    pub id: Option<u32>,
    pub perm: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclResponse {
    pub entries: Vec<AclEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AclActionResponse {
    pub ok: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AclAdapterError {
    InvalidRequest(String),
    Unsupported(String),
    ControlPlane(String),
}

#[async_trait]
pub trait AclAdapter: fmt::Debug + Send + Sync {
    async fn get(
        &self,
        volume_id: &str,
        path: &str,
        runtime: &InstanceRecord,
    ) -> Result<AclResponse, AclAdapterError>;

    async fn put(
        &self,
        volume_id: &str,
        path: &str,
        request: AclResponse,
        runtime: &InstanceRecord,
    ) -> Result<AclResponse, AclAdapterError>;

    async fn delete(
        &self,
        volume_id: &str,
        path: &str,
        runtime: &InstanceRecord,
    ) -> Result<(), AclAdapterError>;
}

pub fn default_acl_adapter() -> Arc<dyn AclAdapter> {
    Arc::new(ControlPlaneAclAdapter)
}

#[derive(Debug)]
struct ControlPlaneAclAdapter;

#[async_trait]
impl AclAdapter for ControlPlaneAclAdapter {
    async fn get(
        &self,
        _volume_id: &str,
        path: &str,
        runtime: &InstanceRecord,
    ) -> Result<AclResponse, AclAdapterError> {
        match send_acl_request(
            runtime,
            ControlRequest::GetAcl {
                path: path.to_string(),
            },
        )
        .await?
        {
            ControlResponse::Acl {
                path: response_path,
                entries,
            } => acl_response(path, &response_path, entries),
            ControlResponse::Error { code, message } => control_error(code, message),
            other => unexpected_response(other),
        }
    }

    async fn put(
        &self,
        _volume_id: &str,
        path: &str,
        request: AclResponse,
        runtime: &InstanceRecord,
    ) -> Result<AclResponse, AclAdapterError> {
        validate_acl_response(&request)?;
        match send_acl_request(
            runtime,
            ControlRequest::PutAcl {
                path: path.to_string(),
                entries: request
                    .entries
                    .into_iter()
                    .map(ControlAclEntry::from)
                    .collect(),
            },
        )
        .await?
        {
            ControlResponse::Acl {
                path: response_path,
                entries,
            } => acl_response(path, &response_path, entries),
            ControlResponse::Error { code, message } => control_error(code, message),
            other => unexpected_response(other),
        }
    }

    async fn delete(
        &self,
        _volume_id: &str,
        path: &str,
        runtime: &InstanceRecord,
    ) -> Result<(), AclAdapterError> {
        match send_acl_request(
            runtime,
            ControlRequest::DeleteAcl {
                path: path.to_string(),
            },
        )
        .await?
        {
            ControlResponse::AclDeleted {
                path: response_path,
            } if response_path == path => Ok(()),
            ControlResponse::AclDeleted {
                path: response_path,
            } => Err(AclAdapterError::ControlPlane(format!(
                "ACL path mismatch: requested {path}, got {response_path}",
            ))),
            ControlResponse::Error { code, message } => control_error(code, message),
            other => unexpected_response(other),
        }
    }
}

async fn send_acl_request(
    runtime: &InstanceRecord,
    request: ControlRequest,
) -> Result<ControlResponse, AclAdapterError> {
    send_acl_request_with_timeout(runtime, request, ACL_CONTROL_TIMEOUT).await
}

async fn send_acl_request_with_timeout(
    runtime: &InstanceRecord,
    request: ControlRequest,
    timeout: Duration,
) -> Result<ControlResponse, AclAdapterError> {
    tokio::time::timeout(timeout, send_request(&runtime.socket_path, &request))
        .await
        .map_err(|_| {
            AclAdapterError::ControlPlane("ACL control-plane request timed out".to_string())
        })?
        .map_err(|err| {
            AclAdapterError::ControlPlane(format!("ACL control-plane request failed: {err}"))
        })
}

fn acl_response(
    requested_path: &str,
    response_path: &str,
    entries: Vec<ControlAclEntry>,
) -> Result<AclResponse, AclAdapterError> {
    if response_path != requested_path {
        return Err(AclAdapterError::ControlPlane(format!(
            "ACL path mismatch: requested {requested_path}, got {response_path}",
        )));
    }
    Ok(AclResponse {
        entries: entries.into_iter().map(AclEntry::from).collect(),
    })
}

fn control_error<T>(code: String, message: String) -> Result<T, AclAdapterError> {
    match code.as_str() {
        "invalid_request" => Err(AclAdapterError::InvalidRequest(message)),
        "unsupported" => Err(AclAdapterError::Unsupported(message)),
        _ => Err(AclAdapterError::ControlPlane(format!("{code}: {message}"))),
    }
}

fn unexpected_response<T>(response: ControlResponse) -> Result<T, AclAdapterError> {
    Err(AclAdapterError::ControlPlane(format!(
        "unexpected ACL control-plane response: {response:?}",
    )))
}

fn validate_acl_response(response: &AclResponse) -> Result<(), AclAdapterError> {
    let entries: Vec<ControlAclEntry> = response
        .entries
        .iter()
        .cloned()
        .map(ControlAclEntry::from)
        .collect();
    validate_acl_entries(&entries).map_err(AclAdapterError::InvalidRequest)
}

impl From<AclEntry> for ControlAclEntry {
    fn from(entry: AclEntry) -> Self {
        Self {
            scope: entry.scope,
            tag: entry.tag,
            id: entry.id,
            perm: entry.perm,
        }
    }
}

impl From<ControlAclEntry> for AclEntry {
    fn from(entry: ControlAclEntry) -> Self {
        Self {
            scope: entry.scope,
            tag: entry.tag,
            id: entry.id,
            perm: entry.perm,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::protocol::{ControlAclEntry, ControlRequest, ControlResponse};
    use crate::control::runtime::InstanceRecord;
    use crate::control::server::{ControlHandler, ControlServer};

    #[tokio::test]
    async fn default_adapter_forwards_acl_requests_to_control_plane() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("control.sock");
        let _server = ControlServer::bind(socket_path.clone(), AclHandler)
            .await
            .unwrap();
        let runtime = InstanceRecord::new(
            std::process::id(),
            "/mnt/brewfs".to_string(),
            socket_path,
            chrono::Utc::now(),
        );
        let adapter = default_acl_adapter();
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

        let get = adapter.get("vol-1", "/docs", &runtime).await.unwrap();
        assert_eq!(get.entries[0].tag, "user_obj");

        let put = adapter
            .put("vol-1", "/docs", request.clone(), &runtime)
            .await
            .unwrap();
        assert_eq!(put, request);

        adapter.delete("vol-1", "/docs", &runtime).await.unwrap();
    }

    #[tokio::test]
    async fn acl_request_times_out_when_control_plane_stalls() {
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

        let err = send_acl_request_with_timeout(
            &runtime,
            ControlRequest::GetAcl {
                path: "/docs".to_string(),
            },
            std::time::Duration::from_millis(10),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            AclAdapterError::ControlPlane(message) if message.contains("timed out")
        ));
    }

    #[test]
    fn validates_acl_entries_before_control_plane_writes() {
        validate_acl_response(&AclResponse {
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
                    scope: "default".to_string(),
                    tag: "group".to_string(),
                    id: Some(1000),
                    perm: "r-x".to_string(),
                },
            ],
        })
        .unwrap();

        assert_acl_validation_error(
            AclEntry {
                scope: "mask".to_string(),
                tag: "user_obj".to_string(),
                id: None,
                perm: "rwx".to_string(),
            },
            "scope",
        );
        assert_acl_validation_error(
            AclEntry {
                scope: "access".to_string(),
                tag: "owner".to_string(),
                id: None,
                perm: "rwx".to_string(),
            },
            "tag",
        );
        assert_acl_validation_error(
            AclEntry {
                scope: "access".to_string(),
                tag: "user".to_string(),
                id: None,
                perm: "rwx".to_string(),
            },
            "requires id",
        );
        assert_acl_validation_error(
            AclEntry {
                scope: "access".to_string(),
                tag: "other".to_string(),
                id: Some(1000),
                perm: "rwx".to_string(),
            },
            "must not include id",
        );
        assert_acl_validation_error(
            AclEntry {
                scope: "access".to_string(),
                tag: "group_obj".to_string(),
                id: None,
                perm: "read".to_string(),
            },
            "perm",
        );
    }

    #[test]
    fn rejects_incomplete_access_acl_base_entries() {
        let err = validate_acl_response(&AclResponse {
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
            ],
        })
        .unwrap_err();

        assert!(matches!(
            err,
            AclAdapterError::InvalidRequest(message) if message.contains("access ACL must include user_obj, group_obj, and other entries")
        ));
    }

    fn assert_acl_validation_error(entry: AclEntry, needle: &str) {
        let err = validate_acl_response(&AclResponse {
            entries: vec![entry],
        })
        .unwrap_err();
        assert!(matches!(
            err,
            AclAdapterError::InvalidRequest(message) if message.contains(needle)
        ));
    }

    #[test]
    fn maps_control_invalid_request_to_adapter_invalid_request() {
        let err = control_error::<()>(
            "invalid_request".to_string(),
            "ACL entry 1 is invalid".to_string(),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            AclAdapterError::InvalidRequest(message) if message == "ACL entry 1 is invalid"
        ));
    }

    struct AclHandler;

    #[async_trait]
    impl ControlHandler for AclHandler {
        async fn handle(&self, request: ControlRequest) -> ControlResponse {
            match request {
                ControlRequest::GetAcl { path } => ControlResponse::Acl {
                    path,
                    entries: vec![ControlAclEntry {
                        scope: "access".to_string(),
                        tag: "user_obj".to_string(),
                        id: None,
                        perm: "rwx".to_string(),
                    }],
                },
                ControlRequest::PutAcl { path, entries } => ControlResponse::Acl { path, entries },
                ControlRequest::DeleteAcl { path } => ControlResponse::AclDeleted { path },
                other => ControlResponse::Error {
                    code: "unexpected".to_string(),
                    message: format!("unexpected request: {other:?}"),
                },
            }
        }
    }
}
