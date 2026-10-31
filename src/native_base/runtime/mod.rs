//! Runtime admission and migration boundaries for `workspace-native-v2`.
//!
//! The runtime recognizes a native volume only after validating both the
//! locator header and the compiled capability set. No caller may reinterpret
//! an unknown native header as `workspace-v1` or `flat-v1`.

#[cfg(feature = "native-packed-base")]
pub mod admin;
mod header;
mod io;
mod migration;
mod object;
pub mod planner;
mod sampler;
#[cfg(feature = "workspace-overlay")]
mod workspace;

#[cfg(all(test, feature = "native-packed-base"))]
pub(crate) mod admin_test_support;
#[cfg(all(test, feature = "native-packed-base"))]
mod admin_tests;

#[cfg(feature = "native-packed-base")]
pub use admin::{
    NativeP1ControlView, NativeP1DomainView, NativeP1HeadView, NativeP1Revision, NativeP1Workspace,
    ensure_p1_control, inspect_p1_control, mark_p1_deleting, p1_storage_namespace_id,
    validate_p1_volume,
};
pub use header::{
    FROZEN_METADATA_FEATURE, NATIVE_CONTROL_VERSION, NATIVE_SCHEMA_VERSION, NATIVE_VOLUME_FORMAT,
    NATIVE_WIRE_MAJOR, NATIVE_WIRE_MINOR, NativeRuntimeCapabilities, NativeVolumeHeader,
    RuntimeAdmissionError, initialize_volume, load_volume_header,
};
pub use io::{
    AcceptedWrite, BaseDataSource, NativeDataRuntime, NativeIoError, RuntimeWriteReceipt,
    ZeroBaseDataSource,
};
pub use migration::{
    MigrationError, MigrationMode, MigrationReport, MigrationRequest, copy_volume_logically,
    validate_migration,
};
pub use object::BackendObjectRepository;
pub use sampler::{Sample, SampleHint, SampleIssuePlan, SamplerError, plan_sample_issue_order};
#[cfg(feature = "workspace-overlay")]
pub use workspace::WorkspaceBaseDataSource;
