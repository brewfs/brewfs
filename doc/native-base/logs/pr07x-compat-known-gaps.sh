#!/usr/bin/env bash
# PR07X compatibility guards and the recorded KnownGaps.
#
#   REGRESS-001 with the native feature OFF the old (flat/chunk) path is
#               unchanged: its whole lib test suite passes in that
#               configuration, and the native feature builds are additive
#   REGRESS-002 a binary that cannot understand the volume refuses it
#               outright -- an older binary (no native support compiled) and
#               a newer volume (schema/wire/format moved) are both refused by
#               name, with no "read it as a flat volume" fallback
#   REGRESS-005 the buffered / direct / mmap shapes are recorded as KnownGaps
#               in doc/native-base/known-gaps.md and are never claimed PASS
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07X: format check ==="
cargo fmt --all --check

echo "=== PR07X: an older binary or a newer volume is refused (REGRESS-002) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  native_base::runtime::header::tests::an_older_binary_or_a_newer_volume_is_refused_without_a_flat_fallback

echo "=== PR07X: the same guard holds in the feature-off (old binary) configuration (REGRESS-002) ==="
cargo test -p brewfs --no-default-features --features fuse-tokio-runtime --lib -- \
  native_base::runtime::header::tests::

echo "=== PR07X: the legacy path is green with the native feature off (REGRESS-001) ==="
cargo test -p brewfs --no-default-features --features fuse-tokio-runtime --lib

echo "=== PR07X: the buffered/direct/mmap KnownGaps are recorded (REGRESS-005) ==="
grep -n 'generic/075' AGENTS.md | head -3
grep -n 'iogen01' AGENTS.md | head -3
grep -n 'ENODEV' AGENTS.md | head -3
grep -c '' doc/native-base/known-gaps.md
grep -n 'KnownGap' doc/native-base/known-gaps.md | head -3

echo "=== PR07X compatibility guards and KnownGaps: ALL PASSED ==="
