//! Permanent publication, exact retention, seal recovery and fork lifecycle.
//!
//! This is the PR06A control-plane boundary.  It deliberately does not
//! contain private-domain deletion: close certificates and cleanup belong to
//! PR06B.  The compact PR04 [`HeadState`](crate::native_base::write::records::HeadState)
//! encoding is left untouched; lifecycle transactions maintain the richer
//! BNCT kind-16 view alongside it.

pub mod cleanup;
pub mod fork;
pub mod index;
pub mod manifest;
pub mod options;
pub mod orphan;
pub mod publish;
pub mod retention;
pub mod seal;
pub mod variant;

#[cfg(test)]
mod tests;

use crate::native_base::wire::error::WireError;
use crate::native_base::write::store::StoreError;

#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("operation id mismatch: {0}")]
    OperationIdMismatch(String),
    #[error("invalid lifecycle state: {0}")]
    InvalidState(String),
    #[error("retention closure rejected: {0}")]
    Retention(String),
    #[error("durability evidence rejected: {0}")]
    Durability(String),
    #[error("object cleanup failed: {0}")]
    Cleanup(String),
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("record decode: {0}")]
    Record(String),
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("wire: {0}")]
    Wire(#[from] WireError),
}

impl LifecycleError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, LifecycleError::Conflict(_))
    }
}

pub type LifecycleResult<T> = Result<T, LifecycleError>;

pub(crate) fn map_conflict(error: StoreError, context: &str) -> LifecycleError {
    match error {
        StoreError::Conflict => LifecycleError::Conflict(context.to_owned()),
        other => LifecycleError::Store(other),
    }
}
