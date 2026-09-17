//! Pure validation for retired lifecycle options.
//!
//! PR07 owns configuration loading, so this function is intentionally not
//! wired into a runtime schema in PR06A and RET-021 remains not-run.

use super::{LifecycleError, LifecycleResult};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyRetentionOptions {
    pub retention_ttl_seconds: Option<u64>,
    pub published_gc: Option<bool>,
    pub read_retention_leases: Option<bool>,
}

pub fn reject_legacy_retention_options(options: &LegacyRetentionOptions) -> LifecycleResult<()> {
    let mut names = Vec::new();
    if options.retention_ttl_seconds.is_some() {
        names.push("retention_ttl_seconds");
    }
    if options.published_gc.is_some() {
        names.push("published_gc");
    }
    if options.read_retention_leases.is_some() {
        names.push("read_retention_leases");
    }
    if names.is_empty() {
        Ok(())
    } else {
        Err(LifecycleError::InvalidState(format!(
            "retired native-base options are not supported: {}",
            names.join(", ")
        )))
    }
}
