#!/usr/bin/env bash
# PR07D admission-gate evidence.
#
# The release/admission refusals that keep unshipped release profiles,
# unverified durability claims, retired retention options and purge/force/age
# cleanup rules out of a native volume:
#   WRITE-013  commit_before_upload is refused for a native volume
#   GATE-001   only the shipped P1 release profiles are admitted
#   GATE-004   durability must be an explicit verified contract
#   RET-021    TTL / published_gc / read_retention_leases fail closed
#   CLN-024    purge / force / age-based cleanup rules are refused
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR07D: format check ==="
cargo fmt --all --check

echo "=== PR07D: config gates (WRITE-013/GATE-001/GATE-004/RET-021/CLN-024) ==="
cargo test -p brewfs --bins config::tests::native

echo "=== PR07D: GATE-004 publication durability refusal ==="
cargo test -p brewfs --features native-packed-base --lib native_base::lifecycle::tests::bounded_drain_rejects_forgery_and_noop_write_barrier_then_recovers

echo "=== PR07D: RET-021 retired-option validator ==="
cargo test -p brewfs --features native-packed-base --lib native_base::lifecycle::tests::retired_retention_options_fail_closed_without_claiming_runtime_wiring

echo "=== PR07D admission gates: ALL PASSED ==="
