#!/usr/bin/env bash
# PR07R open-unlinked inode carry across seal, fork and recovery.
#
#   ORD-009   an inode whose last name is removed while a handle stays open
#             survives the seal inside the same write domain: the new private
#             head carries its canonical attributes and every placement, the
#             baseline and any fork listing stay free of it, reads and writes
#             use the carried placement (no copy-up) and the last close
#             releases it
#   ORD-010   a seal whose publication reply was lost is recovered by
#             OperationId and may only reproduce the single recorded head
#             switch; another head, another payload, another orphan accounting
#             or a non-empty visible_delta_count is refused instead of
#             fast-forwarded over
#   IDX-006   an immediate fork of a sealed base does not see the nameless
#             inode and a fork view that still reports private orphan state is
#             refused, while the sealing domain keeps the handle data
#   LIFE-006  the fork of a sealed base reads every Loose and every Packed
#             object immediately, exactly once each, manifest included
#   WRITE-011 the surviving handle keeps its workspace write domain and its
#             content, and never enters the fork-visible baseline
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07R: format check ==="
cargo fmt --all --check

echo "=== PR07R: open-unlink across seal and close (ORD-009 / WRITE-011) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::orphan::tests::open_unlinked_inode_survives_seal

echo "=== PR07R: fork after seal, loose + packed readable (IDX-006 / LIFE-006) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::orphan::tests::a_fork_after_seal

echo "=== PR07R: lost reply recovery by OperationId (ORD-010) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::orphan::tests::a_lost_seal_reply

echo "=== PR07R orphan carry across seal, fork and recovery: ALL PASSED ==="
