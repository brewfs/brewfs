#!/usr/bin/env bash
# PR07+ focused gate: baseline/loose/hole read merging, the punch-hole commit
# path, and the frozen directory-cursor regressions.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

echo "=== PR07B: format check ==="
cargo fmt --all --check

echo "=== PR07B: native check ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --features native-packed-base

echo "=== PR07B: default feature check (non-regression) ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs

echo "=== PR07B: runtime io focused tests ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --features native-packed-base --lib native_base::runtime::io::tests

echo "=== PR07B: frozen focused tests ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --features native-packed-base --lib native_base::frozen::tests

echo "=== PR07B: native lib tests ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --features native-packed-base --lib

echo "=== PR07B: native clippy ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy -p brewfs --features native-packed-base --lib

echo "=== PR07B focused gate: ALL PASSED ==="
