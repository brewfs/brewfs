#!/usr/bin/env bash
# PR07F ingest-verification evidence.
#
#   VFY-001  the multipart composite is a fact of its own: it is never the
#            ObjectRef full hash, and a composite that disagrees with the
#            locally sealed hash is refused instead of being recorded
#   VFY-004  a resume that changes the part boundaries or the verification
#            profile is refused (old receipts are never mixed into a
#            differently planned attempt) until the new plan is registered
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07F: format check ==="
cargo fmt --all --check

echo "=== PR07F: ingest verification suite (VFY-001/VFY-004) ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::ingest

echo "=== PR07F ingest verification: ALL PASSED ==="
