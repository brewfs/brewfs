#!/usr/bin/env bash
# WSL wrapper for PR06A final gate and raw-log capture.
set -u
set -o pipefail
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/brewfs-target-vigorous-solomon-7caff5"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
repo=/mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5
cd "$repo" || exit 1
if [ -f "$repo/.claude/gate-commit.txt" ]; then
    GATE_COMMIT=$(tr -d '\r\n' < "$repo/.claude/gate-commit.txt")
    export GATE_COMMIT
fi
echo "toolchain: $(cargo --version)"
echo "target dir: $CARGO_TARGET_DIR"
bash doc/native-base/logs/pr06a-final-gate.sh 2>&1 | tee doc/native-base/logs/pr06a-final-gate.log
rc=${PIPESTATUS[0]}
echo "GATE_PIPELINE_EXIT=$rc"
exit "$rc"
