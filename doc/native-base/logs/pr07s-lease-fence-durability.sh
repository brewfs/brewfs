#!/usr/bin/env bash
# PR07S writer-lease fencing, backend lease time and the durability boundary.
#
#   WRITE-007 an expired or superseded lease yields no commit guard, and the
#             fenced write leaves the whole volume namespace untouched: no
#             extent, inode, registration, mutation result or head row appears
#   KV-004    lease validity is decided on backend time; a client clock is
#             refused outright, so no amount of client skew can extend or
#             expire a lease, and a superseded generation is fenced before the
#             clock is even consulted
#   KV-005    every durability profile reports which stages a successful reply
#             confirms and which may still be lost; confirmed stages must be a
#             prefix of the write stages, and an unknown persisted profile code
#             is refused instead of rounded
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07S: format check ==="
cargo fmt --all --check

echo "=== PR07S: lease fence without partial metadata (WRITE-007) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::write::tests::memory_backend::an_expired_or_superseded_lease_is_fenced_without_partial_metadata

echo "=== PR07S: backend lease time vs client skew (KV-004) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::write::lease::tests::lease_validity_comes_from_the_backend_clock

echo "=== PR07S: durability profile loss boundary (KV-005) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::write::lease::tests::durability_profiles_report_their_lossy_boundary

echo "=== PR07S lease fence, backend lease time and durability boundary: ALL PASSED ==="
