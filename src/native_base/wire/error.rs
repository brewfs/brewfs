//! Wire error taxonomy for the native packed base formats.
//!
//! Spec 02/13: unknown format, unknown control version, and unknown enum
//! values must fail closed with an explicit error. This type distinguishes
//! "the bytes are structurally valid but this build cannot interpret them"
//! (`UnsupportedFormat`) from "the bytes violate the format" (`Invalid`),
//! integrity failures, and limit violations.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum WireError {
    /// Structurally recognized but not supported by this build (unknown wire
    /// major, required feature bit, codec, control version, record kind, or
    /// enum value that this reader must reject instead of guessing).
    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),
    /// Bytes violate the wire format (bad magic, reserved bytes set,
    /// non-canonical encoding, out-of-range field, trailing bytes).
    #[error("invalid {what}: {detail}")]
    Invalid { what: &'static str, detail: String },
    /// CRC32C mismatch on a header/footer/envelope.
    #[error("crc32c mismatch on {what}: stored {stored:08x}, computed {computed:08x}")]
    CrcMismatch {
        what: &'static str,
        stored: u32,
        computed: u32,
    },
    /// Digest mismatch (SHA-256 over stored or decoded payload).
    #[error("hash mismatch on {what}: stored {stored}, computed {computed}")]
    HashMismatch {
        what: &'static str,
        stored: String,
        computed: String,
    },
    /// Input ends before the announced structure.
    #[error("truncated input while reading {what}: need {need} bytes, have {have}")]
    Truncated {
        what: &'static str,
        need: usize,
        have: usize,
    },
    /// A hard limit from spec 02 §8 was exceeded; nothing large was allocated.
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),
    /// Compression layer failure (zstd).
    #[error("codec error: {0}")]
    Codec(String),
}

impl WireError {
    pub(crate) fn invalid(what: &'static str, detail: impl Into<String>) -> Self {
        Self::Invalid {
            what,
            detail: detail.into(),
        }
    }
}

pub type WireResult<T> = Result<T, WireError>;
