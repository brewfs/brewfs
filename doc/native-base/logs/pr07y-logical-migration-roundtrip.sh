#!/usr/bin/env bash
# PR07Y logical migration roundtrip (REGRESS-003).
#
#   REGRESS-003 a logical copy republishes every control row of a native
#               volume under a new volume id in ONE conditional transaction,
#               together with the target's locator header.  The source is
#               retained (every row still holds the same payload), attributes
#               and layout have an identical twin under the target, a reader
#               that was never initialized on the target serves the same size
#               and the same bytes from the shared content-addressed objects,
#               and the copy fails closed on a reused volume id, a mode that
#               moves nothing, an occupied target and a store that leaves the
#               scanned prefix.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07Y: format check ==="
cargo fmt --all --check

echo "=== PR07Y: logical migration roundtrip and its refusals (REGRESS-003) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  native_base::runtime::migration::tests

echo "=== PR07Y: the copy and the key prefix it rewrites are part of the surface ==="
grep -n 'copy_volume_logically' src/native_base/runtime/mod.rs
grep -n 'pub async fn copy_volume_logically' src/native_base/runtime/migration.rs
grep -n 'pub fn volume_prefix' src/native_base/write/keys.rs
grep -n 'MigrationReport' src/native_base/runtime/mod.rs

echo "=== PR07Y logical migration roundtrip: ALL PASSED ==="
