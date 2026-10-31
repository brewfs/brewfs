#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
unset TOKIO_WORKER_THREADS
source <(sed '$d' "$SCRIPT_DIR/run_perf_in_container.sh")
[[ ! -v TOKIO_WORKER_THREADS ]] || { echo 'observer changed the default Tokio worker count' >&2; exit 1; }
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
artifact_dir="$test_root/artifact"
mkdir -p "$artifact_dir/diagnostics" "$artifact_dir/results" "$artifact_dir/tools"
mount_dir="$test_root/mount"
mkdir -p "$mount_dir"

# Generate the helper in the test directory without mounting anything. Keep
# Tokio's normal default unless the caller explicitly supplied a worker count.
(
    eval "$(declare -f install_mount_helper | sed "s|/usr/sbin/mount.fuse.brewfs|$test_root/mount-helper|")"
    install_mount_helper
    if rg '^export TOKIO_WORKER_THREADS=' "$test_root/mount-helper"; then
        echo 'observer changed the default Tokio worker count' >&2
        exit 1
    fi
    TOKIO_WORKER_THREADS=3 install_mount_helper
    rg -qx 'export TOKIO_WORKER_THREADS=3' "$test_root/mount-helper"
)

# Observation must preserve the existing throughput profile's cache and
# publication defaults. The separate WS-0 envelope supplies its own overrides.
(
    source <(sed -n '/^enable_writeback_throughput_profile()/,/^}/p' "$SCRIPT_DIR/run_redis_perf.sh")
    unset BREWFS_WRITEBACK_PERSIST_SYNC BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT
    unset BREWFS_METADATA_OPEN_CACHE_TTL_MS
    enable_writeback_throughput_profile
    [[ "$BREWFS_WRITEBACK_PERSIST_SYNC" == false ]]
    [[ "$BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT" == false ]]
    [[ "$BREWFS_METADATA_OPEN_CACHE_TTL_MS" == 1000 ]]
)

# Missing fields and read failures cannot satisfy strict drain.
if numeric_stat_required absent 'present 0'; then
    echo 'missing metric was accepted' >&2
    exit 1
fi
if required_drain_snapshot 'brewfs_buffer_dirty_bytes 0' >/dev/null; then
    echo 'incomplete drain snapshot was accepted' >&2
    exit 1
fi
stats="$(for metric in \
    brewfs_writeback_dirty_bytes brewfs_writeback_live_dirty_bytes \
    brewfs_writeback_live_slices brewfs_writeback_recent_pending_upload_bytes \
    brewfs_writeback_recent_pending_upload_slices brewfs_writeback_stage_inflight_bytes \
    brewfs_writeback_remote_upload_inflight_bytes brewfs_writeback_metadata_inflight \
    brewfs_buffer_dirty_bytes brewfs_s3_put_ops_total brewfs_s3_put_bytes_total \
    brewfs_writeback_recent_uploaded_bytes; do
    printf '%s 0\n' "$metric"
done)"
drain_is_clear "$(required_drain_snapshot "$stats")" 0
if drain_is_clear "$(required_drain_snapshot "${stats/brewfs_writeback_metadata_inflight 0/brewfs_writeback_metadata_inflight 1}")" 0; then
    echo 'pending metadata was accepted as drained' >&2
    exit 1
fi

# Every clear sample comes from one read; three successive samples suffice.
printf '0' >"$test_root/reads"
brewfs_stats_dump() {
    local count
    count="$(cat "$test_root/reads")"
    printf '%s' "$((count + 1))" >"$test_root/reads"
    printf '%s\n' "$stats"
}
stats_snapshot_before_tool() { :; }
stats_snapshot_after_tool() { :; }
sleep() { :; }
wait_for_strict_drain fio-randrw post-write 30 1 0 test >"$test_root/drain.log"
[[ "$(cat "$test_root/reads")" == 3 ]]
PERF_LINES_PROTOCOL=ws0
if wait_for_strict_drain fio-randrw post-write 30 1 0 test >"$test_root/no-backend.log" 2>&1; then
    echo 'protocol drain accepted missing backend evidence' >&2
    exit 1
fi
unset PERF_LINES_PROTOCOL

# A successful Python fallback must not become a real xfstests P-M baseline.
(
    PERF_LINES_PROTOCOL=ws0
    if run_metadata_fallback metaperf "$mount_dir/metaperf" >"$test_root/fallback.log" 2>&1; then
        echo 'protocol accepted Python metadata fallback' >&2
        exit 1
    fi
    [[ -e "$artifact_dir/metadata-implementation.invalid" ]]
)

# Cumulative backend exports must advance independently of client snapshots.
(
    PERF_BACKEND_METRICS_URL=http://test.invalid/snapshot
    python3() {
        local count
        count="$(cat "$test_root/backend-reads")"
        printf '%s' "$((count + 1))" >"$test_root/backend-reads"
        sed -n "$((count + 1))p" "$test_root/backend-values"
    }
    printf '0' >"$test_root/backend-reads"
    printf '1 100 8 9\n1 100 8 9\n2 100 8 9\n3 100 8 9\n' >"$test_root/backend-values"
    wait_for_strict_drain fio-randrw post-write 30 1 0 test >"$test_root/backend-stable.log"
    [[ "$(cat "$test_root/backend-reads")" == 4 ]]
    for regression in '2 101 8 9' '2 100 7 9' '2 100 8 8' '0 100 8 9'; do
        printf '0' >"$test_root/backend-reads"
        printf '1 100 8 9\n%s\n3 100 8 9\n4 100 8 9\n' "$regression" >"$test_root/backend-values"
        if wait_for_strict_drain fio-randrw post-write 30 1 0 test >"$test_root/backend-regression.log" 2>&1; then
            echo "accepted backend reset/decrease: $regression" >&2
            exit 1
        fi
    done
)

# strace is a PATH command, while require_tool_bin requires an absolute path.
(
    mkdir -p "$test_root/bin"
    printf '#!/bin/sh\nexit 0\n' >"$test_root/bin/strace"
    chmod +x "$test_root/bin/strace"
    PATH="$test_root/bin:$PATH"
    PERF_FIO_SYSCALL_TAIL=true
    run_observed_fio regression --name=regression
)

# Named overrides retain the runner's prefill/remount lifecycle and fio args.
PERF_FIO_RANDRW_ARGS='--verify=crc32c'
PERF_FIO_PREFILL_DRAIN=true
PERF_FIO_PREFILL_REMOUNT=true
PERF_FIO_POST_WRITE_DRAIN=false
prepare_fio_dataset() { printf 'prefill\n' >>"$test_root/events"; }
wait_for_fio_prefill_drain() { printf 'drain\n' >>"$test_root/events"; }
remount_brewfs_for_fio_profile() { printf 'remount\n' >>"$test_root/events"; }
run_logged_tool() { printf '%s\n' "$@" >"$test_root/argv"; }
append_fio_log_summary() { :; }
run_fio_profile fio-randrw randrw
[[ "$(cat "$test_root/events")" == $'prefill\ndrain\nremount' ]]
for arg in --lat_percentiles=1 --clat_percentiles=1 --percentile_list=50:95:99:99.9 --randseed=20261031 --randrepeat=1; do
    rg -x --fixed-strings -- "$arg" "$test_root/argv" >/dev/null
done

# Successful drain must not hide fio failure in a function called by `||`.
run_logged_tool() { return 7; }
status=0
run_fio_profile fio-randrw randrw || status=$?
[[ "$status" == 7 ]] || { echo "fio exit 7 was masked by later collection/drain" >&2; exit 1; }

printf '[{"workspace_id":"test","fork_base":"revision-id"}]' >"$test_root/workspaces.json"
write_workspace_lower_identity "$test_root/workspaces.json" test "$test_root/lower.json"
python3 - "$test_root/lower.json" <<'PY'
import json, sys
record = json.load(open(sys.argv[1]))
assert record["lower_manifest_hash"] is None, "a revision JSON hash is not a lower manifest hash"
assert record["lower_manifest_status"] == "unavailable"
assert len(record["fork_base_identity_sha256"]) == 64
PY
echo 'request observation runner regressions passed'
