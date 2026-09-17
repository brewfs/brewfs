#!/usr/bin/env bash
# PR07 focused gate: P1 FUSE mount wiring, init command, and runtime admission
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

echo "=== PR07: format check ==="
cargo fmt --all --check

echo "=== PR07: native check ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs --features native-packed-base

echo "=== PR07: default feature check (non-regression) ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo check -p brewfs

echo "=== PR07: native lib tests ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --features native-packed-base --lib

echo "=== PR07: native clippy ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy -p brewfs --features native-packed-base --lib

echo "=== PR07: git diff check ==="
git diff --check

echo "=== PR07 focused gate: ALL PASSED ==="
