//! Data Seal (`.brfds`) reading: the PR03 exact reader (spec 04).
//!
//! Scope of this module (PR03, per spec 15):
//! - The seal root table directory (`BNSD`) and the four tables:
//!   Bindings, Placements, Objects, Frames — encoding, decoding and key
//!   layout (`BE64(slice_id) || BE32(block_index)` etc.).
//! - `SealBuilder`: deterministic seal assembly with referential
//!   validation (used by tests here and by the PR06A seal writer).
//! - `SealSnapshot` / `SealReader`: the pinned-view read path —
//!   digest-authenticated pages, exact-key BNPG lookup, binding+placement
//!   co-resolution, frame descriptor verification, and the synchronous
//!   planned read executor (`read_range_with`) with byte budgeting and
//!   cooperative cancellation (spec 06).
//!
//! NOT in scope here:
//! - The head KV table consulted before the base seal (PR04).
//! - Frozen extents (PR08) and the real object backends (PR05/07); reads go
//!   through the in-process [`source::ObjectSource`] trait.
//! - The async executor, singleflight waiters, prefetch and Range GET
//!   coalescing (PR10). The synchronous executor here must satisfy the same
//!   interleaving contracts (one fetch per frame per read, no zero fill).
//!
//! Invariants enforced throughout (spec 04 §7, spec 06):
//! - A binding without a placement (or vice versa) is an error, never a
//!   partial result.
//! - Missing frames, missing objects, short reads and digest mismatches are
//!   errors; nothing is ever zero-filled or synthesized.
//! - Packed spans must exactly cover `[0, decoded_len)` — no gap, no
//!   overlap, no fill.
//! - NativeBlockV1 frames carry exactly one span `(0, decoded_len, slot, 0)`;
//!   span lengths live in the decoded domain and never borrow `raw_len`.

pub mod binding;
pub mod builder;
pub mod descriptor;
pub mod error;
pub mod placement;
pub mod plan;
pub mod reader;
pub mod source;
pub mod tables;

#[cfg(test)]
mod tests;

pub use binding::BlockBinding;
pub use builder::{DEFAULT_LEAF_TARGET_BYTES, NATIVE_LOOSE_KIND, SealBuilder};
pub use descriptor::{FRAME_DESCRIPTOR_LEN, FrameDescriptor};
pub use error::{SealError, SealResult};
pub use placement::{
    BlockPlacement, MAX_SPANS, NATIVE_LAYOUT_VERSIONED_FRAMED, Span, validate_native_single_span,
    validate_spans,
};
pub use plan::{
    CancelToken, MAX_PLAN_BLOCKS, RECOMMENDED_MAX_INFLIGHT_DECODED,
    RECOMMENDED_MAX_INFLIGHT_ENCODED, ReadBudget, ReadMetrics,
};
pub use reader::{ResolvedBlock, SealReader, SealSnapshot};
pub use source::{ObjectSource, ObjectSourceError};
pub use tables::{
    SEAL_ROOT_MAGIC, SEAL_ROOT_VERSION, SEAL_TABLE_COUNT, SealRoot, TableId, binding_key,
    frame_key, object_key,
};
