#!/usr/bin/env bash
# PR07E resource-bound evidence.
#
# The reader plan enforces its resource bounds before allocating or fetching,
# and every reservation follows the real buffer lifetime:
#   RES-001  a single authenticated unit bigger than the budget fails at once
#   RES-002  the plan's segment count is bounded before task construction
#   RES-003  success / failure / cancellation release every token exactly once
#   RES-004  prefetch cannot consume the demand reserve
#   RES-005  a 64 MiB decoded native block plus its 4-byte persisted header
#            fits the 65 MiB encoded envelope, and the decoded ceiling stays
#            at 64 MiB
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07E: format check ==="
cargo fmt --all --check

echo "=== PR07E: seal plan/budget/cancel (RES-001/002/003) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::seal::tests

echo "=== PR07E: planner budget + singleflight (RES-003/004) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::planner::tests

echo "=== PR07E: frame envelope limits (RES-005) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::wire::frame::tests

echo "=== PR07E resource bounds: ALL PASSED ==="
