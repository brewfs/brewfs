#!/usr/bin/env bash
# PR07V fenced writers: lease/authority rollback and the head guard.
#
#   CONS-004 an expired or superseded writer lease is fenced, and a rollback
#            (including an authority rollback that takes the ownership domain
#            away) stops publishing and cleans all private state: the dirty
#            references go, the durable uploads are protected by an orphan
#            receipt, and no head, inode, extent, binding, placement or
#            mutation-result row appears
#   CONS-005 a second writer that bypasses the first writer's local gate is
#            refused by the durable head guard - an in-process mutex is not a
#            fence because another writer cannot see it - and the refusal
#            leaves the loser's uploads protected while a writer that
#            re-derives the head from the store can still publish
#
# Both ids are INV-08. Every scenario drives the real overlay and the real
# runtime against the in-process control store; the FUSE mount itself is not
# exercised here (see the scope note in implementation-report.md).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

TESTS="native_base::write::tests_fence"

echo "=== PR07V: format check ==="
cargo fmt --all --check

echo "=== PR07V: an expired or superseded lease fences before any row (CONS-004) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  "$TESTS::an_expired_or_superseded_lease_stops_a_dispatch_before_any_row_is_written"

echo "=== PR07V: a fence between two blocks protects the landed uploads (CONS-004) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  "$TESTS::a_fence_between_two_blocks_protects_the_uploads_that_already_landed"

echo "=== PR07V: an authority rollback stops publishing and cleans private state (CONS-004) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  "$TESTS::an_authority_rollback_stops_the_writer_and_cleans_all_private_state"

echo "=== PR07V: a lapsed lease fences the runtime flush (CONS-004) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  "native_base::runtime::io::tests::a_lapsed_writer_lease_fences_the_runtime_and_cleans_its_private_state"

echo "=== PR07V: a bypassing writer is refused by the head guard (CONS-005) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  "$TESTS::another_writer_bypassing_the_local_gate_is_refused_by_the_head_guard"

echo "=== PR07V: the write and runtime suites stay green ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::write::
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::

echo "=== PR07V fenced writers: ALL PASSED ==="
