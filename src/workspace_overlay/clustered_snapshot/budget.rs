//! Shared hard metadata budget exposed by the clustered-snapshot contract.
//!
//! The implementation lives in the always-on frozen reader module so the
//! streaming catalog can compile without the optional workspace-overlay
//! feature. This module preserves the v2 ownership path and public API.

pub use crate::native_base::frozen::budget::{
    BudgetError, BudgetReservation, MetadataBudget, MetadataBudgetSnapshot,
};
