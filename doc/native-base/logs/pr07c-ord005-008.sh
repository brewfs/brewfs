#!/usr/bin/env bash
# ORD-005 / ORD-008 focused gate: an incomplete predecessor must never be
# skipped at an fsync boundary, and a seal/reordered drain must follow the
# registration order without expanding the plan.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

echo "=== ORD-005/008: format check ==="
cargo fmt --all --check

echo "=== ORD-005: runtime fsync boundary ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --features native-packed-base --lib native_base::runtime::io::tests::ord_005

echo "=== ORD-008: overlay admission-order drain ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --lib native_base::write::tests::memory_backend::reordered_upload_cannot_overtake_its_predecessor

echo "=== ORD-005: overlay blocked-predecessor drain ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --lib native_base::write::tests::memory_backend::failed_predecessor_blocks_successors_instead_of_skipping_them

echo "=== ORD-005/008: runtime + write focused suites ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --features native-packed-base --lib native_base::runtime::io::tests
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --lib native_base::write

echo "=== ORD-005/008: native lib tests ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo test -p brewfs --features native-packed-base --lib

echo "=== ORD-005/008: native clippy ==="
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo clippy -p brewfs --features native-packed-base --lib

echo "=== ORD-005/008 focused gate: ALL PASSED ==="
