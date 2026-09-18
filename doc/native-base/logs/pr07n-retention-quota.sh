#!/usr/bin/env bash
# PR07N retention closure, fixed readonly counters and private quota evidence.
#
#   RET-008  the manifest is retained although nothing points at it, and every
#            transitive container (external metadata index pages, nested paged
#            inventory containers) is retained; a closure that forgets one, or
#            declares a foreign/non-container object, fails closed
#   RET-011  a fixed readonly baseline has fixed read/write counters: the frozen
#            path loads one authenticated page per index level and no KV read,
#            P1 still resolves its baseline with a KV namespace read, and no
#            GC retention-lease RPC can be issued
#   CLN-017  deleting the workspace alias of a published source never widens the
#            private candidate set: protection comes from the frozen certificate
#   CLN-021  close to the hard limit stops new admission while every already
#            accepted ticket keeps its drain/publish allowance
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07N: format check ==="
cargo fmt --all --check

echo "=== PR07N: retention closure and P1 baseline counters (RET-008/RET-011) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::tests::retention_keeps
cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::tests::fixed_readonly_baseline

echo "=== PR07N: frozen fixed-revision baseline counters (RET-011) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::frozen::tests::fixed_readonly_baseline

echo "=== PR07N: alias deletion and quota fence (CLN-017/CLN-021) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::cleanup::tests::a_deleted_alias
cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::cleanup::tests::quota_fence

echo "=== PR07N retention closure and private quota: ALL PASSED ==="
