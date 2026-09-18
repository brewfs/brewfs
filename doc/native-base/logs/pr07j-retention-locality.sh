#!/usr/bin/env bash
# PR07J retention/cleanup locality evidence.
#
#   CLN-007  while a normal discard still has an open/read/decoder task the
#            domain cannot freeze: the close refuses (no terminal drain
#            proof / no CLOSED state), so no plan exists and the private
#            data is untouched
#   RET-002  the newer PublishedRevision takes over the workspace's base
#            pointer, and removing that alias leaves the superseded
#            revision's facts intact (both rows stay Forever-retained)
#   RET-020  a hundred unrelated fork / PublishedRevision / lease rows
#            exist, and the cleanup plan still touches only the domain's
#            own certificate, domain row, and object prefix
#   RET-023  each origin domain's receipt.evidence_root enumerates exactly
#            its own control objects, with no global PublishedRevision scan
#            and no self-hash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07J: format check ==="
cargo fmt --all --check

echo "=== PR07J: lifecycle retention/cleanup locality ==="
cargo test -p brewfs --features native-packed-base --lib -- native_base::lifecycle::

echo "=== PR07J retention locality: ALL PASSED ==="
