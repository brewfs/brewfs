use crate::control::runtime::InstanceRecord;
use async_trait::async_trait;
use serde::Serialize;
use std::{fmt, sync::Arc};

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
    Unsupported(&'static str),
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
    Arc::new(UnsupportedTrashAdapter)
}

#[derive(Debug)]
struct UnsupportedTrashAdapter;

#[async_trait]
impl TrashAdapter for UnsupportedTrashAdapter {
    async fn list(
        &self,
        _volume_id: &str,
        _runtime: &InstanceRecord,
    ) -> Result<TrashList, TrashAdapterError> {
        Err(TrashAdapterError::Unsupported(
            "trash APIs are not implemented for BrewFS volumes yet",
        ))
    }

    async fn restore(
        &self,
        _volume_id: &str,
        _entry_id: &str,
        _runtime: &InstanceRecord,
    ) -> Result<(), TrashAdapterError> {
        Err(TrashAdapterError::Unsupported(
            "trash APIs are not implemented for BrewFS volumes yet",
        ))
    }

    async fn delete(
        &self,
        _volume_id: &str,
        _entry_id: &str,
        _runtime: &InstanceRecord,
    ) -> Result<(), TrashAdapterError> {
        Err(TrashAdapterError::Unsupported(
            "trash APIs are not implemented for BrewFS volumes yet",
        ))
    }
}
