#!/usr/bin/env bash
# PR07K ingest checksum-capability evidence.
#
#   VFY-002  a backend that only echoes the metadata hash it was told is
#            detected by the capability probe; the service-validated profile
#            is refused, so the echo never becomes REMOTE_VERIFIED
#   VFY-003  a service that was never told which checksum algorithm to use
#            (it accepts a part whose bytes contradict the plan) is refused
#            before the object's own bytes are uploaded, with the caller
#            still free to use a full readback
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07K: format check ==="
cargo fmt --all --check

echo "=== PR07K: ingest checksum capability probe (VFY-002/VFY-003) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::ingest

echo "=== PR07K checksum capability: ALL PASSED ==="
