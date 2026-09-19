//! Error classification for the native write pipeline.
//!
//! Retryable errors map to [`WriteError::Conflict`] — the caller re-reads the
//! control plane and re-derives the transaction. Everything else is a stable
//! protocol violation: the operation is reported, never retried as-is, and
//! successors behind it on the same inode are blocked rather than skipped
//! (spec 18 §10).

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// A guarded key changed concurrently; re-read and retry.
    #[error("conflict: {0}")]
    Conflict(String),
    /// The workspace head guard failed: epoch, writer generation or commit
    /// sequence moved — the lease is stale (spec 07 §2).
    #[error("stale head guard: {0}")]
    StaleHeadGuard(String),
    /// The writer lease itself is not usable: it expired on backend time, was
    /// superseded, or was evaluated against a clock that is not the backend's
    /// (KV-004).  Re-acquire the lease; this is never retried as-is.
    #[error("lease fence: {0}")]
    LeaseFence(String),
    /// The mutation order is not the next one for its inode; the operation
    /// arrived out of order and must not land (spec 18 §10).
    #[error("out of order: {0}")]
    OutOfOrder(String),
    /// Same OperationId retried with different content (spec 07 §2).
    #[error("operation id mismatch: {0}")]
    OperationIdMismatch(String),
    /// The ownership domain is not accepting registrations (spec 20 §2).
    #[error("domain not active: {0}")]
    DomainNotActive(String),
    /// The registry already binds this `(namespace, object key)` to a
    /// different ObjectId (spec 01 §6: one key, one identity).
    #[error("registry conflict: {0}")]
    RegistryConflict(String),
    /// An object registration did not match the state captured at dispatch
    /// (spec 20 §6: commit verifies origin domain and attempt).
    #[error("registration mismatch: {0}")]
    RegistrationMismatch(String),
    /// A successor operation is blocked by a failed predecessor; it is
    /// reported, not skipped (spec 18 §10).
    #[error("blocked by failed predecessor: {0}")]
    BlockedByPredecessor(String),
    /// Malformed control-plane record.
    #[error("record decode: {0}")]
    Record(String),
    #[error("object backend: {0}")]
    Object(String),
    #[error("store: {0}")]
    Store(#[from] super::store::StoreError),
    #[error("wire: {0}")]
    Wire(#[from] crate::native_base::wire::error::WireError),
}

impl WriteError {
    /// True when re-reading the control plane and re-deriving the operation
    /// can legitimately succeed (transaction conflicts only).
    pub fn is_retryable(&self) -> bool {
        matches!(self, WriteError::Conflict(_))
    }
}
