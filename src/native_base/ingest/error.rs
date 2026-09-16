//! Ingestion and upload errors (spec 08).
//!
//! Every variant maps to a contract decision in the spec: unsupported
//! sources are refused before any artifact is written, source changes stop
//! publication while keeping the session diagnosable, ambiguous upload
//! results are never resolved by re-issuing identity, and journal damage is
//! reported instead of skipped.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    /// The source kind is outside the P1 ingest range (spec 08 §1):
    /// compressed tar, ZIP, or any archive needing a spool.
    #[error("unsupported source: {0}")]
    UnsupportedSource(String),

    /// The source changed after the inventory was taken (spec 08 §1). The
    /// session stays on disk for diagnosis; publication is refused.
    #[error("source changed after inventory: {0}")]
    SourceChanged(String),

    /// An archive member escaped the target namespace (absolute path,
    /// `..` traversal) — rejected before anything is written (spec 08 §2).
    #[error("archive member escapes the target namespace: {0}")]
    PathEscape(String),

    /// The same normalized path appears twice in one source.
    #[error("duplicate path in source: {0}")]
    DuplicatePath(String),

    /// A hardlink could not be resolved to a grouped source identity.
    #[error("unresolved hardlink: {0}")]
    UnresolvedHardlink(String),

    /// A device/socket/fifo payload cannot become file content.
    #[error("device payload is not ingestable: {0}")]
    DevicePayload(String),

    /// The session directory is owned by another builder (spec 08 §3).
    #[error("session is locked by another owner: {0}")]
    SessionLocked(String),

    /// Not enough local disk for the bounded pack budget (spec 08 §3).
    #[error("insufficient disk space for pack spool: need {needed} bytes, have {available}")]
    InsufficientDisk { needed: u64, available: u64 },

    /// The resume carries a different plan digest / checksum algorithm
    /// than the frozen one (spec 08 §12). A new controlled attempt is
    /// required; old upload receipts must not be mixed in.
    #[error("upload plan changed on resume: {0}")]
    PlanMismatch(String),

    /// A WAL record in the middle of the journal is damaged (spec 08 §9).
    /// Only a truncated or CRC-bad *final* record may be dropped.
    #[error("corrupt journal: {0}")]
    CorruptJournal(String),

    /// The object backend could not prove the outcome of an upload; the
    /// caller must resolve by querying the same identity, never by
    /// re-allocating one (spec 08 §7).
    #[error("ambiguous upload result: {0}")]
    AmbiguousUpload(String),

    /// A 412 precondition failure whose object does not match the
    /// OperationId/length/hash triple — never an idempotent success
    /// (spec 08 §7).
    #[error("precondition failed with different content: {0}")]
    PreconditionMismatch(String),

    /// The complete response signalled failure (possibly embedded in an
    /// HTTP 200) — the object is not REMOTE_VERIFIED (spec 08 §7).
    #[error("multipart complete reported failure: {0}")]
    CompleteFailed(String),

    /// Remote verification failed: the readback or service-validated
    /// checksum does not equal the locally sealed object (spec 08 §8).
    #[error("remote verification failed: {0}")]
    RemoteVerificationFailed(String),

    /// The backend cannot prove it verified checksums; publication must
    /// fall back to a full readback or be refused (spec 08 §8, E06).
    #[error("backend cannot prove checksum verification: {0}")]
    UnprovableChecksum(String),

    /// The requested artifact is not published yet; unpublished session
    /// output is not visible (spec 08 §4/§13).
    #[error("artifact is not published: {0}")]
    NotPublished(String),

    /// The session state machine was asked for an invalid transition.
    #[error("invalid state transition: {0}")]
    InvalidTransition(String),

    /// Local full verification of a sealed object failed (spec 08 §8).
    #[error("local verification failed: {0}")]
    LocalVerificationFailed(String),

    /// I/O or backend failure.
    #[error("io/backend error: {0}")]
    Backend(String),
}

impl IngestError {
    /// Whether retrying the same operation with the same identity could
    /// succeed after the transient condition clears. Source changes, plan
    /// mismatches and corrupt journals are permanent for this session.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            IngestError::AmbiguousUpload(_) | IngestError::Backend(_)
        )
    }
}
