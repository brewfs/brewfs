//! The atomic control-store abstraction the commit pipeline runs on.
//!
//! Every persistent step of the write protocol (upload registration, the
//! extent/binding/placement commit, ownership inventory appends) must be a
//! *single atomic conditional transaction* on the control plane (spec 07 §2,
//! spec 20 §6). This module defines that transaction shape once, so the
//! in-memory, Redis and TiKV backends all implement identical semantics:
//!
//! - [`Expect::Absent`] fails the transaction if the key exists.
//! - [`Expect::Bytes`] fails the transaction unless the current value is
//!   byte-equal — optimistic concurrency on the exact record the caller
//!   derived its writes from.
//! - All writes apply together or not at all.
//!
//! A failed check surfaces as [`StoreError::Conflict`], which is retryable:
//! the caller re-reads the state it based its decision on and re-derives the
//! transaction. Anything else is a backend/protocol failure.

use async_trait::async_trait;

/// The expected state of one key inside a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expect {
    /// The key must not exist.
    Absent,
    /// The key must hold exactly these bytes.
    Bytes(Vec<u8>),
}

/// One atomic conditional transaction: all `checks` must pass, then all
/// `writes` apply together.
#[derive(Debug, Default, Clone)]
pub struct Txn {
    pub checks: Vec<(Vec<u8>, Expect)>,
    /// `None` deletes the key.
    pub writes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl Txn {
    pub fn new() -> Self {
        Self::default()
    }

    /// Require `key` to be absent.
    pub fn check_absent(mut self, key: Vec<u8>) -> Self {
        self.checks.push((key, Expect::Absent));
        self
    }

    /// Require `key` to hold `expected` bytes.
    pub fn check_bytes(mut self, key: Vec<u8>, expected: Vec<u8>) -> Self {
        self.checks.push((key, Expect::Bytes(expected)));
        self
    }

    /// Set `key` to `value`.
    pub fn put(mut self, key: Vec<u8>, value: Vec<u8>) -> Self {
        self.writes.push((key, Some(value)));
        self
    }

    /// Delete `key`.
    pub fn delete(mut self, key: Vec<u8>) -> Self {
        self.writes.push((key, None));
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A checked key did not match. Retryable: re-read and re-derive.
    #[error("transaction conflict: a checked key changed concurrently")]
    Conflict,
    /// Backend/protocol failure.
    #[error("backend error: {0}")]
    Backend(String),
}

impl StoreError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, StoreError::Conflict)
    }
}

/// The control plane the native write pipeline persists to.
///
/// `run` executes a [`Txn`] atomically: either every check passes and every
/// write applies, or nothing applies and [`StoreError::Conflict`] is
/// returned. `scan` returns all key/value pairs under `prefix` in key order
/// (unsigned byte-string order, spec 02 §10); it is a plain read, not
/// transactionally coupled with `run`.
#[async_trait]
pub trait ControlStore: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>;
    async fn run(&self, txn: Txn) -> Result<(), StoreError>;
    async fn scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError>;
}

/// Assert one check against the current value, mirroring the transaction
/// semantics for callers that validate before building a [`Txn`].
pub fn expect_matches(expect: &Expect, current: Option<&[u8]>) -> bool {
    match expect {
        Expect::Absent => current.is_none(),
        Expect::Bytes(expected) => current == Some(expected.as_slice()),
    }
}
