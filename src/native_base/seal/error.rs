//! PR03 read-path errors: wire failures plus source, budget, cancellation
//! and lookup-contract failures that are not wire-format violations.

use thiserror::Error;

use super::source::ObjectSourceError;
use crate::native_base::wire::error::WireError;

#[derive(Debug, Error)]
pub enum SealError {
    /// A wire-format violation while parsing seal structures.
    #[error(transparent)]
    Wire(#[from] WireError),
    /// The object backend failed. Never converted into zeros or absence.
    #[error("object source: {0}")]
    Source(#[from] ObjectSourceError),
    /// A required seal table is absent (spec 04 §2: empty tables use None,
    /// so an absent table queried for a needed key is an error, not empty).
    #[error("seal table {0:?} is absent")]
    MissingTable(super::tables::TableId),
    /// The exact key was not found in the table.
    #[error("key {key} not found in table {table:?}")]
    KeyNotFound {
        table: super::tables::TableId,
        key: String,
    },
    /// Bindings and Placements key sets disagree (spec 04 §3).
    #[error("binding/placement key sets disagree: {0}")]
    KeySetMismatch(String),
    /// A placement references structure that does not resolve (missing
    /// frame descriptor, missing object, kind mismatch, span bounds).
    #[error("invalid placement: {0}")]
    Placement(String),
    /// A frame descriptor disagrees with the actual frame header
    /// (spec 04 §5: ordinal/lengths/codec/format/raw_digest must match).
    #[error("frame descriptor mismatch: {0}")]
    DescriptorMismatch(String),
    /// Content integrity failure at the block level.
    #[error("block integrity: {0}")]
    Integrity(String),
    /// The native inner block could not be decoded.
    #[error("native block decode: {0}")]
    Native(String),
    /// The requested range is outside the block's decoded length
    /// (spec 04 §3).
    #[error("read range {offset}..{end} exceeds block decoded_len {decoded_len}")]
    RangeBeyondBlock {
        offset: u64,
        end: u64,
        decoded_len: u64,
    },
    /// A unit's peak footprint exceeds the hard budget; it is rejected
    /// immediately, never queued forever (spec 06 §7).
    #[error("budget unit too large: needs {needed} bytes against cap {cap}")]
    BudgetUnitTooLarge { needed: u64, cap: u64 },
    /// The read was cancelled; no partial success is returned.
    #[error("read cancelled")]
    Cancelled,
    /// A plan-level limit (segment count) was exceeded.
    #[error("plan limit exceeded: {0}")]
    PlanLimit(String),
}

pub type SealResult<T> = Result<T, SealError>;

impl SealError {
    pub(crate) fn placement(detail: impl Into<String>) -> Self {
        SealError::Placement(detail.into())
    }

    pub(crate) fn integrity(detail: impl Into<String>) -> Self {
        SealError::Integrity(detail.into())
    }
}
