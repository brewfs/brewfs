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

pub mod counterexamples;
