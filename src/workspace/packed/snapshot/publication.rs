//! Bounded physical validation primitives. Publication authority additionally
//! requires namespace/edge/frame relations and durable lifecycle protection.

mod index_context;
mod inventory;
#[cfg(target_os = "linux")]
pub(crate) mod native_effective;
mod payload;
mod physical;
#[cfg(test)]
mod physical_tests;
#[cfg(test)]
mod real_s3_tests;
mod semantic_facts;
pub use index_context::{
    SnapshotIndexAuditCounts, SnapshotIndexAuditLimits, SnapshotIndexContextAudit,
    audit_snapshot_index_contexts,
};
pub(crate) use index_context::{
    SnapshotStagedObjectVerifier, audit_snapshot_staged_index_contexts,
};
pub use payload::{
    AuthenticatedPayload as SnapshotAuthenticatedPayload, PayloadLimits as SnapshotPayloadLimits,
    PayloadSummary as SnapshotPayloadSummary,
    authenticate_payload as authenticate_snapshot_payload,
};
pub use physical::{
    SnapshotPhysicalAuditCounts, SnapshotPhysicalAuditLimits, SnapshotPhysicalDependencyAudit,
    audit_snapshot_physical_dependencies,
};
