#!/usr/bin/env bash
# PR04 bash-script gates, re-run with recorded per-script exit codes.
#
# Why this exists: the PR04 gate script (pr04-gate.sh) runs these same
# scripts with `|| exit 1`, so a failure aborts the gate but a success is
# silent — the original pr04-gate.log therefore records no exit code for
# any of them, and the PR04 evidence table's "0x4 / 0x3" was inferred from
# control flow rather than observed. This script prints one line per
# script with its real exit code, so the claim is backed by a recorded
# value. It mirrors pr01/pr02/pr03-bash-gates.sh.
#
# The compose-xfstests scripts are run on an LF-normalized copy in /tmp
# because this Windows checkout converts docker/**.sh to CRLF while Linux
# CI checks them out as LF. The repository copies are not touched.
set -u

SRC=/mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5
G=/tmp/brewfs-gate-pr04-bash
LOG="$SRC/doc/native-base/logs/pr04-bash-gates.log"
overall=0

rm -rf "$G"
mkdir -p "$G/docker" || exit 1
cp -r "$SRC/docker/compose-xfstests" "$G/docker/" || exit 1
find "$G" -name '*.sh' -exec sed -i 's/\r$//' {} + || exit 1

{
    cd "$G/docker/compose-xfstests" || exit 1
    for s in run_perf_in_container.sh run_redis_perf.sh run_juicefs_perf_in_container.sh run_juicefs_perf.sh; do
        bash -n "$s"
        echo "bashn:$s:exit=$?"
    done
    for s in test_perf_report_delta.sh test_juicefs_direct_matrix.sh test_juicefs_perf_report.sh; do
        bash "$s"
        echo "test:$s:exit=$?"
    done
} 2>&1 | tee "$LOG"

# Re-derive the overall status from the recorded lines.
grep -qE ':exit=[^0]' "$LOG" && overall=1
echo "PR04-BASH-GATES overall=$overall"
exit $overall
