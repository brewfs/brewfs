#!/usr/bin/env bash
# PR08B: one plain frame shared by two files (OPT-001 / INV-04).
#
#   OPT-001 "Plain shared frame": small files aggregated into one plain
#               frame must slice per file -- each file sees only its own
#               bytes, across spans that are non-adjacent in the frame --
#               and the decoded budget for such a read is the frame, once,
#               not once per span or per referencing file.  This run drives
#               the new seal fixture for both claims, including the
#               one-byte-short refusal before any I/O.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR08B: format check ==="
cargo fmt --all --check

echo "=== PR08B: one plain frame, two files, per-file slices (OPT-001) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  native_base::seal::tests::one_plain_frame_serves_two_files_without_mixing_their_slices

echo "=== PR08B: the shared frame's decoded budget is one frame (OPT-001) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  native_base::seal::tests::decoded_budget_for_a_shared_plain_frame_counts_the_frame_once

echo "=== PR08B: the shared-frame unit and its accounting are in the read plan ==="
grep -n 'UnitKey::Frame(span.frame_slot)' src/native_base/seal/plan.rs
grep -n 'decoded_payload_bytes += raw.len() as u64;' src/native_base/seal/plan.rs
grep -n 'fn shared_plain_frame_fixture' src/native_base/seal/tests.rs
grep -n 'fn decoded_budget_for_a_shared_plain_frame_counts_the_frame_once' \
  src/native_base/seal/tests.rs
grep -c '' src/native_base/seal/tests.rs

echo "=== PR08B plain shared frame: ALL PASSED ==="