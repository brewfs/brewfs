use thiserror::Error;

use super::ids::{LayerId, LeaseId, WorkspaceId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConflictDetail {
    pub path: Vec<u8>,
    pub reason: String,
}

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("feature not compiled: {0}")]
    FeatureNotCompiled(&'static str),
    #[error("unsupported volume format: {0}")]
    UnsupportedVolumeFormat(String),
    #[error("unsupported workspace schema version: {0}")]
    UnsupportedSchemaVersion(u32),
    #[error("workspace backend lacks required capability: {0}")]
    UnsupportedCapability(&'static str),
    #[error("workspace not found: {0}")]
    WorkspaceNotFound(WorkspaceId),
    #[error("layer not found: {0}")]
    LayerNotFound(LayerId),
    #[error("lease not found: {0}")]
    LeaseNotFound(LeaseId),
    #[error("workspace is busy")]
    Busy,
    #[error("workspace writer was fenced")]
    Fenced,
    #[error("workspace commit conflict at {path:?}: {reason}", path = .0.path, reason = .0.reason)]
    Conflict(ConflictDetail),
    #[error("layer depth {depth} exceeds hard limit {hard_limit}")]
    LayerDepthLimit { depth: u32, hard_limit: u32 },
    #[error("corrupt workspace metadata: {0}")]
    CorruptMetadata(String),
    #[error("invalid read plan: {0}")]
    InvalidReadPlan(String),
    #[error("invalid state transition from {from} to {to}")]
    InvalidStateTransition { from: String, to: String },
    #[error("workspace backend error: {0}")]
    Backend(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
