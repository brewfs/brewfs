#!/usr/bin/env bash
# PR07I private-cleanup evidence.
#
#   CLN-001  objects superseded or failed inside a still-ACTIVE domain are
#            never physical DELETE candidates: planning needs a CLOSED
#            domain row with a frozen certificate, and a stale certificate
#            cannot re-open the door
#   CLN-004  a domain is only cleanable once CLOSED and its inventory is
#            frozen; a legal dispatch landing afterwards is refused rather
#            than guessed at
#   CLN-014  a DELETE that landed but lost its response keeps the batch
#            journal and releases the freed-byte statistic exactly once
#   CLN-015  a partially failed batch is counted object by object: the
#            landed deletes are recorded, the failed one stays pending, and
#            the retry only charges the remainder
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07I: format check ==="
cargo fmt --all --check

echo "=== PR07I: private cleanup candidate/accounting gates ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::cleanup

echo "=== PR07I private cleanup: ALL PASSED ==="
