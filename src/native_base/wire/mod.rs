//! Read-only wire 3/0 codecs for the native packed base formats
//! (specs 02, 03, 20).
//!
//! PR02 scope — codec skeleton with Rust goldens and negative cases:
//!
//! - [`uvarint`]: canonical shortest-form LEB128 plus the shared
//!   `Reader`/`Writer` for all length-prefixed scalars (bool, Option, vec,
//!   i64 two's complement LE).
//! - [`container`]: 64-byte header/footer, object kinds/magics,
//!   `required_features` bits, CRC32C.
//! - [`refs`]: PageAddress (56B), ObjectRef, RootRef, ChildRef with key
//!   validation and level-descent checks.
//! - [`frame`]: 80-byte FrameHeader, PlainBytes/NativeBlockV1 payload
//!   formats, None/Zstd outer codecs with exact-length enforcement.
//! - [`datapack`]: `.brfdp` scrub parser and deterministic builder; the
//!   four fixture goldens round-trip byte-for-byte.
//! - [`page`]: BNPG index pages (generic key/value kind only).
//! - [`bnct`]: native control records, envelope version 2, kinds 1–7 and
//!   16–19, fail-closed on unknown versions/kinds/enum values.
//!
//! Explicitly **not** in this PR: Data Seal / Frozen Metadata / Snapshot
//! Manifest payload layouts (specs 04/05), Zstd-compressed index pages
//! beyond the shared frame codec, and any KV placement. The supported codec
//! surface is exactly what the tests assert against the packaged fixtures.
//!
//! No `transmute`, no serde default enum ordering, no naturally aligned
//! struct persistence (spec 02 §1, §10).

pub mod bnct;
pub mod container;
pub mod datapack;
pub mod error;
pub mod frame;
pub mod page;
pub mod refs;
pub mod uvarint;

pub use error::{WireError, WireResult};

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;

    /// The packaged fixtures must remain byte-identical to what the specs
    /// ship; these hashes come from examples/wire-vectors.json and
    /// examples/control-vectors.json (INV-01, WIRE-002).
    #[test]
    fn packaged_fixture_hashes_match_spec_vectors() {
        let vectors: &[(&str, &[u8], &str)] = &[
            (
                "empty.brfdp",
                include_bytes!("../testdata/empty.brfdp"),
                "9c4e03e7a8a08ca8013717f91182b75b9f1b5f7f2c30cf61d6e5e2258fffee8f",
            ),
            (
                "two_plain_frames.brfdp",
                include_bytes!("../testdata/two_plain_frames.brfdp"),
                "6f20c4290976af232ac98b04aa7e345d2fb91af71750675ad95b4bcf5a116710",
            ),
            (
                "native_none.brfdp",
                include_bytes!("../testdata/native_none.brfdp"),
                "204dec25f7bbbcf251c09cad7344d225bcbd990837242549b6bc9abb1e22f34e",
            ),
            (
                "plain_magic_prefix.brfdp",
                include_bytes!("../testdata/plain_magic_prefix.brfdp"),
                "b358e5fb371a56d3f92f999c844f4f9b3167a6f5858f488ef1e4fed94cce9dcc",
            ),
            (
                "kv_base_retention.bnct",
                include_bytes!("../testdata/kv_base_retention.bnct"),
                "1959c861fb1c60abbf506e5e38e3a59329c21f56ec95784e334fdfe2fb4759e3",
            ),
        ];
        for (name, bytes, sha) in vectors {
            let digest = sha2::Sha256::digest(bytes);
            assert_eq!(hex::encode(digest), *sha, "{name}");
        }
    }

    /// Every fixture must survive the full scrub/decode path (INV-04).
    #[test]
    fn all_brfdp_fixtures_scrub_clean() {
        for bytes in [
            include_bytes!("../testdata/empty.brfdp").as_slice(),
            include_bytes!("../testdata/two_plain_frames.brfdp").as_slice(),
            include_bytes!("../testdata/native_none.brfdp").as_slice(),
            include_bytes!("../testdata/plain_magic_prefix.brfdp").as_slice(),
        ] {
            datapack::ScrubbedPack::scrub(bytes).unwrap_or_else(|e| panic!("scrub failed: {e}"));
        }
        let bnct = include_bytes!("../testdata/kv_base_retention.bnct");
        bnct::ControlRecord::decode(bnct).unwrap();
    }
}
