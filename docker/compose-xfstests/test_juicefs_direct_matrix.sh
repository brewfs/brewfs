#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(realpath "$SCRIPT_DIR/../..")"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

export BREWFS_ARTIFACT_DIR="$tmpdir/artifact"
export JFS_MOUNT_POINT="$tmpdir/mount"
mkdir -p "$BREWFS_ARTIFACT_DIR/results" "$BREWFS_ARTIFACT_DIR/tools" "$JFS_MOUNT_POINT"

# Source all functions without running main.
source <(sed '$d' "$REPO_DIR/docker/compose-xfstests/run_juicefs_perf_in_container.sh")

artifact_dir="$BREWFS_ARTIFACT_DIR"
mount_dir="$JFS_MOUNT_POINT"
calls_file="$tmpdir/run-logged-tool-calls.tsv"

run_logged_tool() {
    local tool="$1"
    shift
    printf '%s\t%s\n' "$tool" "$*" >>"$calls_file"
    return 0
}

append_fio_log_summary() { :; }
wait_for_fio_post_write_drain() { :; }

export PERF_FIO_RUNTIME=1
export PERF_FIO_DIRECT_MATRIX="0 1"
run_fio_profile "fio-seqwrite" seqwrite

line_count="$(wc -l <"$calls_file" | tr -d ' ')"
if [[ "$line_count" != "2" ]]; then
    echo "expected direct matrix to run 2 fio profiles, got $line_count" >&2
    cat "$calls_file" >&2
    exit 1
fi

grep -Fq -- 'fio-seqwrite-direct0' "$calls_file"
grep -Fq -- '--direct=0' "$calls_file"
grep -Fq -- 'fio-seqwrite-direct1' "$calls_file"
grep -Fq -- '--direct=1' "$calls_file"

: >"$calls_file"
export PERF_FIO_DIRECT_MATRIX="0 2"
if run_fio_profile "fio-seqwrite" seqwrite >"$tmpdir/invalid.out" 2>&1; then
    echo "expected invalid direct matrix value to fail" >&2
    cat "$calls_file" >&2
    exit 1
fi

grep -Fq -- '无效的 fio direct matrix 值: 2' "$tmpdir/invalid.out"
