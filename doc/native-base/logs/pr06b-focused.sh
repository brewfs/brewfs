#!/usr/bin/env bash
# Focused PR06B evidence: close/cleanup counterexamples and legacy-delete guards.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/brewfs-target-vigorous-solomon-7caff5}"
repo=/mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5
cd "$repo" || exit 1

cargo test -p brewfs --lib native_base::lifecycle::cleanup -- --test-threads 4
cleanup_rc=$?
echo "cleanup:exit=$cleanup_rc"
cargo test -p brewfs --lib native_base::lifecycle -- --test-threads 4
lifecycle_rc=$?
echo "lifecycle:exit=$lifecycle_rc"
cargo test -p brewfs --lib chunk::store::tests::native_v2_rejects_legacy_block_range_deletion -- --test-threads 4
guard_rc=$?
echo "legacy-delete-guard:exit=$guard_rc"
cargo clippy -p brewfs --lib -- -D warnings
clippy_rc=$?
echo "clippy:exit=$clippy_rc"

if [ "$cleanup_rc" -ne 0 ] || [ "$lifecycle_rc" -ne 0 ] || [ "$guard_rc" -ne 0 ] || [ "$clippy_rc" -ne 0 ]; then
    exit 1
fi
