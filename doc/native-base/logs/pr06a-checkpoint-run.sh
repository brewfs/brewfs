#!/usr/bin/env bash
# Wrapper: runs the PR06A checkpoint gate inside WSL with the toolchain env
# this branch uses, capturing the log into doc/native-base/logs/.
set -u
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/brewfs-target-vigorous-solomon-7caff5"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
R=/mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5
cd "$R" || exit 1
# WSL git cannot read the Windows worktree, so the Windows-side shell writes
# the SHA here before invoking this wrapper.
if [ -f "$R/.claude/gate-commit.txt" ]; then
    GATE_COMMIT=$(tr -d '\r\n' < "$R/.claude/gate-commit.txt")
    export GATE_COMMIT
fi
echo "toolchain: $(cargo --version)"
echo "target dir: $CARGO_TARGET_DIR"
bash doc/native-base/logs/pr06a-checkpoint-gate.sh 2>&1 | tee doc/native-base/logs/pr06a-checkpoint-gate.log
echo "GATE_PIPELINE_EXIT=${PIPESTATUS[0]}"
