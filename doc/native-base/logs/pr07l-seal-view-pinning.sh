#!/usr/bin/env bash
# PR07L sealed-view locator and pinning evidence.
#
#   READ-005  an index page that is not loaded yet is fetched through its
#             locator: the local children of an external page are addressed
#             inside the external container, and a page that cannot be loaded
#             is an error rather than an inferred absence
#   READ-006  one pinned view never mixes revisions: after the container
#             changes underneath the reader the pinned view keeps the
#             revision its pages were authenticated against, and a view
#             opened after the switch fails closed
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07L: format check ==="
cargo fmt --all --check

echo "=== PR07L: sealed view locator and pinning (READ-005/READ-006) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::seal::tests

echo "=== PR07L sealed view locator and pinning: ALL PASSED ==="
