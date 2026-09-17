#!/usr/bin/env bash
# Focused PR06A evidence: lifecycle counterexamples followed by the complete
# native-base module suite.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/brewfs-target-vigorous-solomon-7caff5"
repo=/mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5
cd "$repo" || exit 1

cargo test -p brewfs --lib -j 4 -- native_base::lifecycle:: --test-threads 4
first=$?
echo "lifecycle:exit=$first"
cargo test -p brewfs --lib -j 4 -- native_base:: --test-threads 4
second=$?
echo "native_base:exit=$second"
if [ "$first" -ne 0 ] || [ "$second" -ne 0 ]; then
    exit 1
fi
