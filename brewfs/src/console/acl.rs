use crate::control::runtime::InstanceRecord;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

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
    Unsupported(&'static str),
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
    Arc::new(UnsupportedAclAdapter)
}

#[derive(Debug)]
struct UnsupportedAclAdapter;

#[async_trait]
impl AclAdapter for UnsupportedAclAdapter {
    async fn get(
        &self,
        _volume_id: &str,
        _path: &str,
        _runtime: &InstanceRecord,
    ) -> Result<AclResponse, AclAdapterError> {
        Err(AclAdapterError::Unsupported(
            "ACL control-plane adapter is not implemented yet",
        ))
    }

    async fn put(
        &self,
        _volume_id: &str,
        _path: &str,
        _request: AclResponse,
        _runtime: &InstanceRecord,
    ) -> Result<AclResponse, AclAdapterError> {
        Err(AclAdapterError::Unsupported(
            "ACL control-plane adapter is not implemented yet",
        ))
    }

    async fn delete(
        &self,
        _volume_id: &str,
        _path: &str,
        _runtime: &InstanceRecord,
    ) -> Result<(), AclAdapterError> {
        Err(AclAdapterError::Unsupported(
            "ACL control-plane adapter is not implemented yet",
        ))
    }
}
