#!/usr/bin/env bash
# CTRL-004 focused gate: a close copy whose hash disagrees with the
# authoritative KV certificate stops the close/cleanup before any write.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

echo "=== CTRL-004: format check ==="
cargo fmt --all --check

echo "=== CTRL-004: lifecycle cleanup focused tests ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --features native-packed-base --lib native_base::lifecycle::cleanup::tests::ctrl_004

echo "=== CTRL-004: lifecycle focused tests ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --features native-packed-base --lib native_base::lifecycle::

echo "=== CTRL-004: native lib tests ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --features native-packed-base --lib

echo "=== CTRL-004: native clippy ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy -p brewfs --features native-packed-base --lib

echo "=== CTRL-004 focused gate: ALL PASSED ==="
