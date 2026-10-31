//! Workspace-isolated copy-on-write views.
//!
//! This module is compiled only when the `workspace-overlay` feature is
//! explicitly enabled. The default flat-volume path must not depend on it.

pub mod admin;
pub mod cache;
pub mod cache_scope;
pub mod catalog;
// Packed-v2 clustered snapshots are retired; packed-v3 is the only packed format.
pub mod control;
pub mod digest;
pub mod entity;
pub mod error;
pub mod gc;
pub mod ids;
pub mod lifecycle;
pub mod meta_layer;
pub mod metrics;
pub mod model;
pub mod packed_reader_lifecycle;
pub mod packed_shutdown;
#[cfg(test)]
pub(crate) mod test_base_fixture;
pub use stores::kv_store::packed_admin;
pub mod packed_v3;
pub mod publish;
pub mod resolver;
pub mod stores;
