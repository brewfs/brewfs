#!/usr/bin/env bash
# CTRL-003 focused gate: empty-domain RetainBatch and close-certificate
# evidence (Option::None expresses the empty set).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

echo "=== CTRL-003: format check ==="
cargo fmt --all --check

echo "=== CTRL-003: lifecycle focused tests ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --features native-packed-base --lib native_base::lifecycle::

echo "=== CTRL-003: native lib tests ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --features native-packed-base --lib

echo "=== CTRL-003: native clippy ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy -p brewfs --features native-packed-base --lib

echo "=== CTRL-003 focused gate: ALL PASSED ==="
