//! BrewFS native packed base (`workspace-native-v2`).
//!
//! Implementation entry for the 1.2-consolidated native base specification
//! (DataPack / Data Seal / Frozen Metadata staged delivery, exact published
//! retention, and end-of-domain private cleanup). See
//! `doc/native-base/README.md` for the PR plan and the acceptance matrix
//! tracked in this repository.
//!
//! PR01 scope: counterexample models only. The wire codec (PR02), readers
//! (PR03), commit pipeline (PR04) and retention transactions (PR06A/06B)
//! land in later PRs and must satisfy the contracts recorded here.
//!
//! PR02 scope: read-only wire 3/0 codecs (container header/footer, frame,
//! page, DataPack scrub/build) and the BNCT native control records v2,
//! with goldens for the packaged fixtures and fail-closed negatives.
//!
//! PR03 scope: the Data Seal (`.brfds`) exact reader — table directory,
//! bindings/placements/objects/frames records, SealBuilder with referential
//! validation, the pinned-view SealReader with exact-key BNPG lookup, and
//! the synchronous planned read executor with byte budgeting and
//! cooperative cancellation (spec 04, spec 06).
//!
//! PR04 scope: the write pipeline — the control-store transaction model
//! (memory / Redis / TiKV), the `nb2` key layout, ownership domains and
//! upload registration, the type 3 receipts container, the atomic commit
//! transaction with the per-inode ordering gate, and the write overlay
//! (admission tickets, dirty handoff, ordered drain) that turns a
//! reordered upload into a permanent error (spec 07, spec 18, spec 20).
//!
//! PR05 scope: local/seekable-tar ingest — the unified source contract
//! with consistency revalidation, the BNWL journaled session state
//! machine, the stable frozen upload plan with resume digest checks, the
//! multipart ambiguity-resolution protocol over the object-backend
//! capability interface, and the two remote-verification profiles,
//! stopping at BUILT_UNPUBLISHED so unpublished artifacts stay invisible
//! (spec 08).

pub mod counterexamples;
pub mod ingest;
pub mod seal;
pub mod wire;
pub mod write;
