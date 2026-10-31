//! Experimental current-entity format. No legacy head, sequence or mutation gate.
//!
//! The experiment includes authenticated packed fallthrough, namespace transactions,
//! file I/O and bounded recovery. Production mount admission remains closed.

pub mod backend;
pub mod codec;
pub mod data;
pub(crate) mod experiment;
pub mod keys;
pub mod lifecycle;
pub mod memory;
pub mod model;
pub mod namespace;
pub mod native_lower;
pub mod ownership;
pub mod packed_lower;
pub mod recovery;
pub mod redis;
pub mod resolver;
pub mod writeback;

#[cfg(test)]
mod local_test_objects;
#[cfg(test)]
mod namespace_tests;
#[cfg(test)]
pub(crate) mod test_objects;

pub type Result<T> = std::result::Result<T, EntityError>;

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum EntityError {
    #[error("invalid entity operation: {0}")]
    Invalid(String),
    #[error("corrupt entity metadata: {0}")]
    Corrupt(String),
    #[error("entity backend failure (outcome may be unknown): {0}")]
    Backend(String),
    #[error("writer or lifecycle authority fenced")]
    Fenced,
    #[error("affected entity changed; prepare the entire operation again")]
    Conflict,
    #[error("entity not found")]
    NotFound,
    #[error("entity already exists")]
    Exists,
    #[error("permission denied")]
    Permission,
    #[error("not a directory")]
    NotDirectory,
    #[error("is a directory")]
    IsDirectory,
    #[error("directory not empty")]
    NotEmpty,
    #[error("entity busy")]
    Busy,
    #[error("unsupported: {0}")]
    Unsupported(&'static str),
    #[error("integer overflow")]
    Overflow,
    #[error("allocator exhausted")]
    Exhausted,
}
