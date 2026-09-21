#!/usr/bin/env bash
# Cache-budget parity guard for the Aliyun native writeback-throughput profile.
#
# The 2026-09-17 two-leg round produced archives whose budgets did not match:
# BrewFS ran 4 GiB of read cache plus 4 GiB of write cache resident in memory
# under a 12 GiB memory budget, while JuiceFS ran a 4 GiB memory buffer with an
# 8 GiB disk cache. The BrewFS leg then measured its own host, not the filesystem
# -- 175 MiB free and 32% average iowait on a 16 GiB box -- so its read numbers
# were not comparable with JuiceFS.
#
# This test runs the real --writeback-throughput-profile branch of
# run_native_perf.sh for both legs and asserts that each one asks for the same
# budget in the memory tier and in the disk tier. It extracts the parser from the
# runner instead of copying it, so it cannot drift from the thing it describes.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$SCRIPT_DIR/aliyun/run_native_perf.sh"
[[ -f "$SRC" ]] || { echo "FAIL: missing $SRC"; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

tr -d '\r' < "$SRC" > "$tmp/run.sh"
bash -n "$tmp/run.sh" && echo "SYNTAX-OK : bash -n"

sed -n '/^while \[\[ \$# -gt 0 \]\]/,/^done$/p' "$tmp/run.sh" > "$tmp/parser.sh"
grep -q 'writeback-throughput-profile' "$tmp/parser.sh" \
    || { echo "FAIL: parser extraction lost the profile flag"; exit 1; }

cat > "$tmp/harness.sh" <<'HEADER'
set -Eeuo pipefail
die() { echo "DIE: $*" >&2; exit 1; }
log() { :; }
is_truthy() { case "${1:-}" in 1|true|TRUE|yes|YES|on|ON) return 0 ;; *) return 1 ;; esac; }
require_memory_headroom() { echo "HEADROOM required=${1} ${2}"; }
DATA_ROOT=/mnt/data
REDIS_DIR=/tmp/r; RUSTFS_DIR=/tmp/s; LOG_ROOT=/tmp/l; S3_PORT=9000; REDIS_PORT=6379
PERF_TOOLS_VALUE=default
DATA_BACKEND_VALUE=local-fs
mode_report() {
    local mode="$1"; shift
    (
        MODE="$mode"; SOURCE_ROOT=/src
HEADER
cat "$tmp/parser.sh" >> "$tmp/harness.sh"
cat >> "$tmp/harness.sh" <<'FOOTER'
        export PERF_TOOLS="$PERF_TOOLS_VALUE"
        # One line per leg: the unused leg's knobs stay unset in the environment,
        # so mixing them into a single line would only report which one a previous
        # iteration happened to export.
        echo "BUDGET $MODE backend=$DATA_BACKEND_VALUE read=${BREWFS_READ_MEMORY_BYTES:-unset} write=${BREWFS_WRITE_MEMORY_BYTES:-unset} read_ssd=${BREWFS_READ_SSD_BYTES:-unset} write_ssd=${BREWFS_WRITE_SSD_BYTES:-unset} budget=${BREWFS_MEMORY_BUDGET_BYTES:-unset} buffer_mib=${JFS_BUFFER_SIZE_MIB:-unset} cache_mib=${JFS_CACHE_SIZE_MIB:-unset} read_direct=${BREWFS_FUSE_READ_DIRECT_IO:-unset}"
    )
}
mode_report brewfs --tools "fio-bigread" --s3 --writeback-throughput-profile
mode_report juicefs --tools "fio-bigread" --s3 --writeback-throughput-profile
FOOTER

bash -n "$tmp/harness.sh" || { echo "FAIL: generated harness is not valid bash"; exit 1; }
out="$(bash "$tmp/harness.sh")"
printf '%s\n' "$out"

field() {
    local value
    value="$(tr ' ' '\n' <<<"$1" | sed -n 's/^'"$2"'=//p' | head -n 1)"
    [[ -n "$value" && "$value" != unset ]] || {
        echo "FAIL: leg did not set $2" >&2
        exit 1
    }
    printf '%s' "$value"
}

brewfs_line="$(grep -m1 '^BUDGET brewfs ' <<<"$out")"
juicefs_line="$(grep -m1 '^BUDGET juicefs ' <<<"$out")"
brewfs_demand="$(grep -m1 '^HEADROOM ' <<<"$out" | sed -n 's/^HEADROOM required=\([0-9]*\).*/\1/p')"

[[ -n "$brewfs_line" && -n "$juicefs_line" && -n "$brewfs_demand" ]] \
    || { echo "FAIL: could not read the budget lines back"; exit 1; }

# BrewFS splits each tier between its read and write cache; JuiceFS has one knob
# per tier. Compare the totals, which is what the host and the object store see.
brewfs_mem=$(( $(field "$brewfs_line" read) + $(field "$brewfs_line" write) ))
brewfs_disk=$(( $(field "$brewfs_line" read_ssd) + $(field "$brewfs_line" write_ssd) ))
juicefs_mem=$(( $(field "$juicefs_line" buffer_mib) * 1048576 ))
juicefs_disk=$(( $(field "$juicefs_line" cache_mib) * 1048576 ))

echo
echo "brewfs  memory=$(( brewfs_mem / 1048576 ))MiB disk=$(( brewfs_disk / 1048576 ))MiB budget=$(( $(field "$brewfs_line" budget) / 1048576 ))MiB"
echo "juicefs memory=$(( juicefs_mem / 1048576 ))MiB disk=$(( juicefs_disk / 1048576 ))MiB"

status=0
if [[ "$brewfs_mem" != "$juicefs_mem" ]]; then
    echo "FAIL: memory tier differs: brewfs=$(( brewfs_mem / 1048576 ))MiB juicefs=$(( juicefs_mem / 1048576 ))MiB"
    status=1
else
    echo "OK   memory tier aligned at $(( brewfs_mem / 1048576 ))MiB"
fi
if [[ "$brewfs_disk" != "$juicefs_disk" ]]; then
    echo "FAIL: disk tier differs: brewfs=$(( brewfs_disk / 1048576 ))MiB juicefs=$(( juicefs_disk / 1048576 ))MiB"
    status=1
else
    echo "OK   disk tier aligned at $(( brewfs_disk / 1048576 ))MiB"
fi

# The local compose runner treats writeback-throughput as the complete tuned
# profile and enables read-only FOPEN_DIRECT_IO. Keep the native cloud runner
# aligned so old kernels do not split each large buffered read into 256 KiB
# FUSE requests while the local comparison uses 1 MiB direct-I/O requests.
if [[ "$(field "$brewfs_line" read_direct)" != 1 ]]; then
    echo "FAIL: BrewFS cloud writeback profile did not enable read direct-I/O"
    status=1
else
    echo "OK   BrewFS cloud writeback profile enables read direct-I/O"
fi

# The memory pre-check has to ask for the BrewFS caches plus the fio prefill, so
# a budget change that moves the caches but not the guard fails here.
if [[ "$brewfs_demand" != "$(( brewfs_mem + 4294967296 ))" ]]; then
    echo "FAIL: memory pre-check demands ${brewfs_demand}B but the caches plus the 4GiB fio prefill need $(( (brewfs_mem + 4294967296) / 1048576 ))MiB"
    status=1
else
    echo "OK   memory pre-check demand covers caches plus the 4GiB fio prefill"
fi

[[ $status -eq 0 ]] && echo "ALL-OK"
exit $status
