//! Workspace-isolated copy-on-write views.
//!
//! This module is compiled only when the `workspace-overlay` feature is
//! explicitly enabled. The default flat-volume path must not depend on it.

pub mod cache;
pub mod cache_scope;
pub mod catalog;
pub mod compaction;
pub mod control;
pub mod digest;
pub mod error;
pub mod gc;
pub mod ids;
pub mod lifecycle;
pub mod meta_layer;
pub mod metrics;
pub mod model;
pub mod publish;
pub mod resolver;
pub mod stores;
