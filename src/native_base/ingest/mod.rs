//! BrewFS Native Packed Base — local and seekable-tar ingest (spec 08,
//! PR05).
//!
//! The ingest pipeline turns a local directory or an uncompressed
//! seekable tar into a stable, resumable, verifiable upload:
//!
//! * [`source`] — the unified input contract: an inventory of raw
//!   names, entry kinds, attributes, hardlink groups, sparse ranges and
//!   source tokens; exact `read_range`; `revalidate`. Two consistency
//!   policies (`SnapshotBacked`, `BestEffortDetected`); a source that
//!   changed after inventory stops publication.
//! * [`error`] — the contract-decision error surface.
//! * [`wal`] — the BNWL journal and the checkpoint dance: every session
//!   state transition is durable before it takes effect; only a
//!   truncated or CRC-bad final record may be dropped.
//! * [`backend`] — the object-backend capability interface (atomic
//!   create-only PUT, exact range reads, multipart with service-side
//!   part verification and fully parsed complete responses) plus the
//!   fault-injecting in-memory fake used to prove the ambiguity rules.
//! * [`upload`] — the upload executor: stable plans, receipts, and the
//!   §7 ambiguity-resolution protocol (412 idempotence, timeouts
//!   resolved by query, HTTP-200 embedded failures) with the two §8
//!   remote-verification profiles.
//! * [`plan`] — the frozen upload plan and its digest; resume may not
//!   silently change the plan or the part-checksum algorithm.
//! * [`session`] — the session directory layout, the builder state
//!   machine, exclusive ownership and resume.
//! * [`build`] — the phased builder: inventory → freeze → upload →
//!   BUILT_UNPUBLISHED (the P1 default stop; unpublished artifacts are
//!   not visible).

pub mod backend;
pub mod build;
pub mod error;
pub mod plan;
pub mod session;
pub mod source;
pub mod upload;
pub mod wal;

pub use build::{BuildOutcome, FrameOrigin, IngestBuildConfig, IngestBuilder};
pub use error::IngestError;
pub use plan::{PLAN_ALGORITHM_VERSION, UploadPlan, UploadPlanDigest};
pub use session::{Session, SessionState};
pub use source::{
    ConsistencyPolicy, EntryKind, IngestSource, LocalDirSource, SeekableTarSource, SourceEntry,
};
pub use upload::{PutMode, RemoteVerificationProfile, UploadExecutor};
