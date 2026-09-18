#!/usr/bin/env bash
# PR07G read-amortization evidence.
#
#   OPT-002  a hundred same-host concurrent readers of the same immutable
#            frame schedule exactly one fetch/decode and share one buffer,
#            because the runtime's singleflight is the only entry to the
#            loader and every waiter receives the same Arc
#   OPT-004  the metadata-only path (stat/readdir, i.e. Runtime::size) stays
#            on the control plane: no object GET, no object PUT, and no
#            baseline data copy, even after the cache is cleared
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07G: format check ==="
cargo fmt --all --check

echo "=== PR07G: shared read scheduling (OPT-002) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::planner::tests

echo "=== PR07G: metadata-only path (OPT-004) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests

echo "=== PR07G read amortization: ALL PASSED ==="
