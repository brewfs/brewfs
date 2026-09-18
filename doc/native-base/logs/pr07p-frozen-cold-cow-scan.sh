#!/usr/bin/env bash
# PR07P cold attribute eviction, COW page reuse and summary scan accounting.
#
#   FROZEN-005  attributes evicted from RAM are reloaded from the authenticated
#               pages and come back identical (mode/uid/gid and permissions
#               included); negatives are never cached and capacity 0 changes
#               no answer
#   FROZEN-006  a copy-on-write metadata rewrite reuses every unchanged page,
#               writes changed pages into fresh ranges, keeps the old snapshot
#               readable, and refuses an in-place overwrite of a page the old
#               snapshot still references
#   FROZEN-008  a summary metadata scan reports the scanned volume and the
#               rewritten volume separately, on real page slots, and refuses a
#               rewrite that was never scanned
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07P: format check ==="
cargo fmt --all --check

echo "=== PR07P: cold attribute eviction (FROZEN-005) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::cold_attribute_eviction

echo "=== PR07P: COW page reuse (FROZEN-006) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::cow_page_reuse

echo "=== PR07P: summary meta scan accounting (FROZEN-008) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::summary_meta_scan

echo "=== PR07P cold eviction, COW reuse and scan accounting: ALL PASSED ==="
