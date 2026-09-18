#!/usr/bin/env bash
# PR07H capability-closure evidence.
#
#   GATE-002  a container whose declaration only claims None/plain cannot
#             smuggle a Zstd frame or external index child past capability
#             admission: the closure is content-derived, valid CRCs and
#             digests do not help a lying header. The four shipped golden
#             packs keep scrubbing with unchanged declarations.
#   GATE-003  the dependency closure is summarized (declared + content) and
#             enforced by the full verify of a DataPack scrub and of a Data
#             Seal open, i.e. before any byte of the object is served.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07H: format check ==="
cargo fmt --all --check

echo "=== PR07H: container closure primitive (GATE-002/GATE-003) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::wire::container::tests

echo "=== PR07H: DataPack capability admission + shipped goldens (GATE-002) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::wire::datapack::tests

echo "=== PR07H: Data Seal closure before the object is served (GATE-003) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::seal::

echo "=== PR07H capability closure: ALL PASSED ==="
