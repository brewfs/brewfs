#!/usr/bin/env bash
# Capture PR06B final gate with the exact committed source revision.
set -u
set -o pipefail
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/brewfs-target-vigorous-solomon-7caff5}"
repo=/mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5
cd "$repo" || exit 1
echo "gate-commit: ${GATE_COMMIT:-unknown (pass Windows-side git SHA as GATE_COMMIT)}"
echo "toolchain: $(cargo --version)"
echo "target dir: $CARGO_TARGET_DIR"
bash doc/native-base/logs/pr06b-final-gate.sh 2>&1 | tee doc/native-base/logs/pr06b-final-gate.log
rc=${PIPESTATUS[0]}
echo "GATE_PIPELINE_EXIT=$rc"
exit "$rc"
