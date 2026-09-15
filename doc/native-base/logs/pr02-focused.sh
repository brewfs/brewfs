#!/usr/bin/env bash
# PR02 focused test run: fmt + native_base wire/counterexample tests.
set -u
. "$HOME/.cargo/env" 2>/dev/null || true
cd /mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5 || exit 1
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/brewfs-target-vigorous-solomon}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0

echo "== cargo fmt --all --check =="
cargo fmt --all --check
echo "fmt exit: $?"

echo "== cargo test -p brewfs --lib native_base =="
cargo test -p brewfs --lib native_base 2>&1 | tail -40
