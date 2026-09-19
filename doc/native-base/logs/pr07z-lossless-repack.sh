#!/usr/bin/env bash
# PR07Z lossless repack keeps the logical identity (OPT-007).
#
#   OPT-007 a lossless repack moves one block into a NEW pack with a different
#               frame split while the Bindings table -- every BlockKey,
#               decoded_len and content_hash, and therefore the seal's logical
#               revision -- stays byte-identical; both views serve the same
#               bytes, the old object is neither re-read nor rewritten, the
#               variant is booked as extra space on top of the untouched
#               source, and a placement that keeps the declared binding but
#               decodes to different bytes is refused by a full decode against
#               that binding, not by a frame checksum.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07Z: format check ==="
cargo fmt --all --check

echo "=== PR07Z: a lossless repack keeps the bindings and the logical revision (OPT-007) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  native_base::seal::tests::lossless_repack_keeps_bindings_and_logical_revision

echo "=== PR07Z: the pieces the acceptance case pins ==="
grep -n 'fn table_digest' src/native_base/seal/tests.rs
grep -n 'content hash mismatch' src/native_base/seal/plan.rs
grep -n 'layout variant changes logical revision or block bindings' \
  src/native_base/lifecycle/variant.rs
grep -n 'relocated attributes are not byte-identical' src/native_base/frozen/mod.rs

echo "=== PR07Z lossless repack: ALL PASSED ==="
