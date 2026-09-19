#!/usr/bin/env bash
# PR08A training-mode sampler hints keep the sample set and the order
# (OPT-006 / INV-02).
#
#   OPT-006 training mode may take explicit sample / byte-range hints so the
#               underlying issue order can be made sequential, but the hints
#               must not change the sampler's sample set, its distribution or
#               the semantic order the application observes.  This run drives
#               the new planner over a batch that repeats one draw, makes the
#               hints reorder a file, and checks every refusal.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"

export PATH="/home/luxian/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/luxian/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "=== PR08A: format check ==="
cargo fmt --all --check

echo "=== PR08A: hints only reorder the issue order (OPT-006) ==="
cargo test -p brewfs --features native-packed-base --lib -- \
  native_base::runtime::sampler::tests

echo "=== PR08A: the contract the hints may not break ==="
grep -n 'the semantic order the application observes' src/native_base/runtime/sampler.rs
grep -n 'HintNotASample' src/native_base/runtime/sampler.rs | head -3
grep -n 'plan_sample_issue_order' src/native_base/runtime/mod.rs
grep -c '' src/native_base/runtime/sampler.rs

echo "=== PR08A training sampler hints: ALL PASSED ==="
