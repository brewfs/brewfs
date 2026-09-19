#!/usr/bin/env bash
# PR07U: one read request, one bounded and coherent metadata capture.
#
#   CONS-001 a commit landing inside a request's capture (while its extent scan
#             is in flight) is absorbed without serving stale bytes and without
#             mixing an inode row from one generation with extents from another
#   CONS-002 the dirty -> committed handoff happens only once the commit is
#             confirmed: a commit whose reply is lost is retried, answered from
#             the recorded result, and the pending overlay is released exactly
#             then, so the bytes a reader sees never vanish in between
#   CONS-003 one read request reuses a single capture however many blocks,
#             chunks and extents it spans, and its metadata scan count does not
#             grow with the request; two syscalls are two captures, which is
#             the honest boundary of the guarantee
#   CONS-006 a metadata plane that keeps moving or exceeds the attempt timeout
#             is retried only within the request's bounded budget; the reader
#             never holds its state gate across metadata I/O, so a concurrent
#             commit is never blocked behind a capture
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07U: format check ==="
cargo fmt --all --check

echo "=== PR07U: the whole runtime read/write contract ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io

echo "=== PR07U: a commit inside a capture is never served stale (CONS-001) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::a_commit_inside_a_read_capture_is_never_served_stale

echo "=== PR07U: a lost commit reply hands off without a gap (CONS-002) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::a_lost_commit_reply_hands_off_without_a_gap

echo "=== PR07U: one request, one bounded capture (CONS-003) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::one_read_request_reuses_a_single_bounded_capture

echo "=== PR07U: an unstable inode row is bounded (CONS-006) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::an_inode_row_that_never_settles_fails_within_the_bounded_budget

echo "=== PR07U: a metadata timeout is bounded (CONS-006) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::tests::a_metadata_read_beyond_the_attempt_timeout_is_bounded

echo "=== PR07U capture consistency: ALL PASSED ==="
