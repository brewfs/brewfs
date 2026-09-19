#!/usr/bin/env bash
# PR07W write-path acceptance: no copy-up, fresh cross-block slices, the
# dirty->committed handoff and durability across a remount.
#
#   WRITE-001 the first 4096 B overwrite of a GiB file must not copy the file
#             up front: the commit reads exactly the block it replaces,
#             uploads one data object (plus its receipts container) and
#             publishes one extent, while the file keeps its GiB length
#   WRITE-002 a write that straddles a native block boundary publishes fresh
#             slices for the two blocks it covers and rewrites no others
#   WRITE-004 a local read after write returns sees the dirty bytes, and after
#             the commit the same bytes: the handoff is at the commit
#             acknowledgement, with no gap in between
#   WRITE-005 after fsync, clearing the cache and remounting the volume (a
#             fresh runtime over the same store, sink and baseline) serves the
#             same bytes and sizes
#
# Every scenario drives the real runtime; the FUSE mount itself is not
# exercised here (see the scope note in implementation-report.md).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

TESTS="native_base::runtime::io::tests"

echo "=== PR07W: format check ==="
cargo fmt --all --check

echo "=== PR07W: a first 4 KiB overwrite of a GiB file copies nothing (WRITE-001) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  "$TESTS::a_first_4k_overwrite_of_a_gib_file_touches_one_block_and_no_more"

echo "=== PR07W: a cross-block partial write rewrites only its two blocks (WRITE-002) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  "$TESTS::a_write_straddling_a_block_boundary_rewrites_only_those_two_blocks"

echo "=== PR07W: the dirty -> committed handoff has no gap (WRITE-004) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  "$TESTS::a_local_read_sees_the_dirty_bytes_and_then_the_committed_ones"
cargo test -p brewfs --features native-packed-base --lib -- \
  "$TESTS::patch_is_dirty_visible_and_fsync_reads_only_the_touched_block"
cargo test -p brewfs --features native-packed-base --lib -- \
  "$TESTS::a_lost_commit_reply_hands_off_without_a_gap"

echo "=== PR07W: fsync then cache clear and remount keeps bytes and sizes (WRITE-005) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  "$TESTS::fsync_then_cache_clear_and_remount_serves_the_same_bytes_and_sizes"

echo "=== PR07W: the runtime read/write suite stays green ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::runtime::io::

echo "=== PR07W write path and durability: ALL PASSED ==="
