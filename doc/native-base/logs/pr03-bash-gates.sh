#!/usr/bin/env bash
# PR03 bash-script gates, run on an LF-normalized copy in /tmp because the
# Windows checkout converted docker/**.sh to CRLF (Linux CI checks them out
# as LF). Original repo files are untouched.
set -u
SRC=/mnt/d/code/brewfs/.claude/worktrees/vigorous-solomon-7caff5
G=/tmp/brewfs-gate-pr03
LOG="$SRC/doc/native-base/logs/pr03-bash-gates.log"

rm -rf "$G"
mkdir -p "$G/docker"
cp -r "$SRC/docker/compose-xfstests" "$G/docker/"
find "$G" -name '*.sh' -exec sed -i 's/\r$//' {} +

{
    cd "$G/docker/compose-xfstests" || exit 1
    for s in run_perf_in_container.sh run_redis_perf.sh run_juicefs_perf_in_container.sh run_juicefs_perf.sh; do
        if bash -n "$s" 2>&1; then
            echo "bashn:$s:PASS:0"
        else
            echo "bashn:$s:FAIL:$?"
        fi
    done
    for t in test_perf_report_delta.sh test_juicefs_direct_matrix.sh test_juicefs_perf_report.sh; do
        if bash "$t" 2>&1; then
            echo "test:$t:PASS:0"
        else
            echo "test:$t:FAIL:$?"
        fi
    done
} | tee "$LOG"
