#!/usr/bin/env bash
# PR07M runtime read composition and concurrency identity evidence.
#
#   READ-001  one read that spans packed baseline blocks, a loose patch and a
#             hole matches a byte-for-byte oracle (the baseline is a real
#             .brfds pack read through the planned seal reader)
#   READ-007  the same object id in two namespaces is a different identity:
#             never merged into one physical range and never shared through
#             the singleflight
#   READ-008  cancelling waiters of a concurrent scatter leaves no dangling
#             buffer reference and no parked waiter behind
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07M: format check ==="
cargo fmt --all --check

echo "=== PR07M: runtime read composition (READ-001) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io

echo "=== PR07M: read identity and cancelled scatter (READ-007/READ-008) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::planner

echo "=== PR07M runtime read composition: ALL PASSED ==="
