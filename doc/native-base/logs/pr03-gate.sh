#!/usr/bin/env bash
# PR03 AGENTS.md CI gate (cargo side), executed in WSL Ubuntu-24.04.
# Usage: bash doc/native-base/logs/pr03-gate.sh   (from the repo root)
# Writes a full raw log next to this script.
# bash-script gates run separately via pr03-bash-gates.sh (LF-normalized /tmp copies);
# git diff --check runs on the Windows side (WSL git cannot follow the gitdir).
set -u
. "$HOME/.cargo/env" 2>/dev/null || true
cd "$(dirname "$0")/../../.." || exit 1

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/brewfs-target-vigorous-solomon-7caff5}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

LOG="$(dirname "$0")/pr03-gate.log"
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
run "cargo check --workspace" cargo check --workspace
run "cargo build --workspace" cargo build --workspace
run "check fuse-tokio-runtime" cargo check -p brewfs --no-default-features --features fuse-tokio-runtime
run "check fuse-io-uring-runtime" cargo check -p brewfs --no-default-features --features fuse-io-uring-runtime
run "cargo test --workspace --lib --bins" cargo test --workspace --lib --bins
run "cargo clippy --workspace" cargo clippy --workspace

echo "==============================================================" >> "$LOG"
echo "GATE SUMMARY: pass=$pass fail=$fail"
printf '%s\n' "${SUMMARY[@]}" | column -t -s'|'
exit "$fail"
