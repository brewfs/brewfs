//! Runtime admission and migration boundaries for `workspace-native-v2`.
//!
//! The runtime recognizes a native volume only after validating both the
//! locator header and the compiled capability set. No caller may reinterpret
//! an unknown native header as `workspace-v1` or `flat-v1`.

mod header;
mod io;
mod migration;
mod object;
pub mod planner;
#[cfg(feature = "workspace-overlay")]
mod workspace;

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
#[cfg(feature = "workspace-overlay")]
pub use workspace::WorkspaceBaseDataSource;
