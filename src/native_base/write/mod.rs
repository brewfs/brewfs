//! The native write pipeline: admission, ordering, uploads, receipts and
//! the atomic commit transaction (spec 07, spec 18, spec 20).
//!
//! PR04 scope:
//!
//! - [`store`]/[`memory`]: the control-store abstraction — one
//!   compare-and-write transaction model over memory, Redis (Lua, one
//!   script) and TiKV (pessimistic transactions);
//! - [`keys`]/[`records`]: the `nb2/{volume}/` key layout and the compact
//!   record codecs (head state, inode data, extents, head placements);
//! - [`domain`]: ownership domains and the register → dispatch upload
//!   reservation (spec 20 §6);
//! - [`receipts`]: the type 3 single-value receipts container;
//! - [`commit`]: the single all-or-nothing commit transaction — extent +
//!   binding + placement + inode + head, with the per-inode ordering gate
//!   (spec 18 §10) and OperationId idempotency (spec 07 §2);
//! - [`overlay`]: the write overlay — admission tickets, dirty tracking
//!   with handoff, and the in-order drain that makes a reordered upload
//!   unable to land.
//!
//! Tests live in [`tests`] (store-agnostic scenarios, run against the
//! in-memory store in the normal test pass) and `tests_redis`/
//! `tests_tikv` (the same scenarios against real instances, behind
//! `--ignored` integration gates).

pub mod commit;
pub mod domain;
pub mod error;
pub mod keys;
pub mod lease;
pub mod memory;
pub mod overlay;
pub mod receipts;
pub mod records;
pub mod redis;
pub mod store;
pub mod tikv;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_redis;
#[cfg(test)]
mod tests_tikv;
