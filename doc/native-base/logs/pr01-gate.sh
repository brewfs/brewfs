#!/usr/bin/env bash
# PR01 AGENTS.md CI gate, executed in WSL Ubuntu-24.04 (Linux toolchain).
# Usage: bash doc/native-base/logs/pr01-gate.sh   (from the repo root)
# Writes a full raw log next to this script and prints an exit-code table.
set -u
cd "$(dirname "$0")/../../.." || exit 1

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

LOG="$(dirname "$0")/pr01-gate.log"
: > "$LOG"

pass=0
fail=0
declare -a SUMMARY

run() {
    local name="$1"
    shift
    {
        echo "=============================================================="
        echo "\$ $*"
    } >> "$LOG"
    if "$@" >> "$LOG" 2>&1; then
        SUMMARY+=("PASS|$name|0")
        pass=$((pass + 1))
    else
        SUMMARY+=("FAIL|$name|$?")
        fail=$((fail + 1))
    fi
}

run "cargo fmt --all --check" cargo fmt --all --check

run "bash -n run_perf_in_container.sh" bash -n docker/compose-xfstests/run_perf_in_container.sh
run "bash -n run_redis_perf.sh" bash -n docker/compose-xfstests/run_redis_perf.sh
run "bash -n run_juicefs_perf_in_container.sh" bash -n docker/compose-xfstests/run_juicefs_perf_in_container.sh
run "bash -n run_juicefs_perf.sh" bash -n docker/compose-xfstests/run_juicefs_perf.sh
run "test_perf_report_delta.sh" bash docker/compose-xfstests/test_perf_report_delta.sh
run "test_juicefs_direct_matrix.sh" bash docker/compose-xfstests/test_juicefs_direct_matrix.sh
run "test_juicefs_perf_report.sh" bash docker/compose-xfstests/test_juicefs_perf_report.sh

run "cargo check --workspace" cargo check --workspace
run "cargo build --workspace" cargo build --workspace

run "check fuse-tokio-runtime" cargo check -p brewfs --no-default-features --features fuse-tokio-runtime
run "check fuse-io-uring-runtime" cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime

run "cargo test --workspace --lib --bins" cargo test --workspace --lib --bins
run "cargo clippy --workspace" cargo clippy --workspace

run "git diff --check" git diff --check

echo "==============================================================" >> "$LOG"
echo "GATE SUMMARY: pass=$pass fail=$fail"
printf '%s\n' "${SUMMARY[@]}" | column -t -s'|'
exit "$fail"
