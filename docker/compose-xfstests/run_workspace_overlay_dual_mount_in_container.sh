#!/usr/bin/env bash

set -euo pipefail

bin="${BREWFS_BIN:-/usr/local/bin/brewfs}"
state_dir="${BREWFS_WORKSPACE_STATE_DIR:-/var/lib/brewfs}"
catalog_url="${BREWFS_WORKSPACE_CATALOG_URL:-sqlite:workspace.db}"
catalog_backend="${BREWFS_WORKSPACE_META_BACKEND:-sqlx}"
catalog_tikv_endpoints="${BREWFS_WORKSPACE_TIKV_PD_ENDPOINTS:-pd:2379}"
catalog_namespace="${BREWFS_WORKSPACE_NAMESPACE:-brewfs}"
data_dir="${BREWFS_DATA_DIR:-$state_dir/data}"
artifact_dir="${BREWFS_ARTIFACT_DIR:-/artifacts/workspace-dual-mount}"
mount_a="${BREWFS_WORKSPACE_MOUNT_A:-/mnt/brewfs-a}"
mount_b="${BREWFS_WORKSPACE_MOUNT_B:-/mnt/brewfs-b}"
workspace_a="${BREWFS_WORKSPACE_A:?BREWFS_WORKSPACE_A is required}"
workspace_b="${BREWFS_WORKSPACE_B:-}"
mode="${BREWFS_WORKSPACE_HARNESS_MODE:-verify}"

pid_a=""
pid_b=""
last_pid=""
writer_pids=()
concurrency="${BREWFS_WORKSPACE_CONCURRENCY:-8}"
[[ "$concurrency" =~ ^[1-9][0-9]*$ ]] || {
    printf 'BREWFS_WORKSPACE_CONCURRENCY must be a positive integer\n' >&2
    exit 2
}
writer_stop_file="$artifact_dir/writers.stop"
catalog_args=(
    --meta-backend "$catalog_backend"
    --workspace-namespace "$catalog_namespace"
)
case "$catalog_backend" in
    sqlx|redis) catalog_args+=(--meta-url "$catalog_url") ;;
    tikv) catalog_args+=(--meta-tikv-pd-endpoints "$catalog_tikv_endpoints") ;;
    *) printf 'unsupported workspace catalog backend: %s\n' "$catalog_backend" >&2; exit 2 ;;
esac

run_cli() {
    (cd "$state_dir" && "$bin" workspace "${catalog_args[@]}" "$@")
}

log() { printf '[workspace-overlay] %s\n' "$*"; }

is_mounted() {
    findmnt -rn --target "$1" --output FSTYPE 2>/dev/null | grep -Eq '^fuse(\.|$)'
}

stop_mount() {
    local pid="$1"
    local target="$2"
    local status=0

    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill -INT "$pid" 2>/dev/null || true
        for _ in $(seq 1 100); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
    fi
    if is_mounted "$target"; then
        fusermount3 -u "$target" 2>/dev/null || umount "$target" 2>/dev/null || true
    fi
    if [[ -n "$pid" ]]; then
        wait "$pid" 2>/dev/null || status=$?
    fi
    return "$status"
}

cleanup() {
    local status=$?
    set +e
    for pid in "${writer_pids[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    stop_mount "$pid_b" "$mount_b"
    stop_mount "$pid_a" "$mount_a"
    if is_mounted "$mount_a" || is_mounted "$mount_b"; then
        log "a mountpoint remained mounted during cleanup"
        status=1
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

start_mount() {
    local workspace="$1"
    local target="$2"
    local cache_dir="$3"
    local log_file="$4"
    local fuse_log_file="${log_file%.log}.fuse.log"
    mkdir -p "$target" "$cache_dir" "$(dirname "$log_file")"
    (
        cd "$state_dir"
        exec env \
            XDG_CACHE_HOME="$cache_dir" \
            BREWFS_FUSE_LOG_FILE="$fuse_log_file" \
            "$bin" mount \
            --privileged \
            --volume-format workspace-v1 \
            --workspace "$workspace" \
            --data-backend local-fs \
            --data-dir "$data_dir" \
            "${catalog_args[@]}" \
            "$target"
    ) >"$log_file" 2>&1 &
    last_pid=$!
}

wait_for_mount() {
    local pid="$1"
    local target="$2"
    local log_file="$3"
    local deadline=$((SECONDS + 60))

    while (( SECONDS < deadline )); do
        if is_mounted "$target"; then
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            log "mount process exited before $target became ready"
            sed -n '1,240p' "$log_file" >&2 || true
            return 1
        fi
        sleep 0.1
    done
    log "timed out waiting for mount at $target"
    sed -n '1,240p' "$log_file" >&2 || true
    return 1
}

seed_base() {
    log "seeding workspace $workspace_a"
    printf 'shared-base\n' >"$mount_a/shared-base.txt"
    printf '0123456789abcdef' >"$mount_a/base-data.bin"
    python3 - "$mount_a/mmap-base.bin" <<'PY'
import pathlib
import sys

pathlib.Path(sys.argv[1]).write_bytes(b"x" * 4096)
PY
    sync "$mount_a/shared-base.txt" "$mount_a/base-data.bin" "$mount_a/mmap-base.bin"
}

verify_isolation() {
    [[ -n "$workspace_b" ]] || {
        log "BREWFS_WORKSPACE_B is required in verify mode"
        return 2
    }

    cmp -s "$mount_a/shared-base.txt" "$mount_b/shared-base.txt"
    cmp -s "$mount_a/base-data.bin" "$mount_b/base-data.bin"
    [[ "$(stat -c %i "$mount_a/base-data.bin")" == "$(stat -c %i "$mount_b/base-data.bin")" ]]

    printf 'workspace-a\n' >"$mount_a/a-only.txt"
    [[ ! -e "$mount_b/a-only.txt" ]]
    printf 'workspace-b\n' >"$mount_b/b-only.txt"
    [[ ! -e "$mount_a/b-only.txt" ]]

    printf 'changed-in-a\n' >"$mount_a/shared-base.txt"
    grep -qx 'shared-base' "$mount_b/shared-base.txt"

    ln "$mount_a/a-only.txt" "$mount_a/a-hardlink.txt"
    [[ "$(stat -c %i "$mount_a/a-only.txt")" == "$(stat -c %i "$mount_a/a-hardlink.txt")" ]]
    [[ ! -e "$mount_b/a-hardlink.txt" ]]

    setfattr -n user.workspace -v a "$mount_a/a-only.txt"
    [[ "$(getfattr --only-values -n user.workspace "$mount_a/a-only.txt")" == a ]]
    if getfattr --only-values -n user.workspace "$mount_b/shared-base.txt" >/dev/null 2>&1; then
        log "workspace B unexpectedly observed workspace A xattr"
        return 1
    fi

    python3 - "$mount_a/mmap-base.bin" <<'PY'
import mmap
import os
import sys

fd = os.open(sys.argv[1], os.O_RDWR)
try:
    with mmap.mmap(fd, 4096) as mapping:
        mapping[128:132] = b"AGNT"
        mapping.flush()
finally:
    os.close(fd)
PY
    [[ "$(dd if="$mount_a/mmap-base.bin" bs=1 skip=128 count=4 status=none)" == AGNT ]]
    [[ "$(dd if="$mount_b/mmap-base.bin" bs=1 skip=128 count=4 status=none)" == xxxx ]]

    fallocate --punch-hole --keep-size --offset 4 --length 4 "$mount_a/base-data.bin"
    python3 - "$mount_a/base-data.bin" "$mount_b/base-data.bin" <<'PY'
import pathlib
import sys

a = pathlib.Path(sys.argv[1]).read_bytes()
b = pathlib.Path(sys.argv[2]).read_bytes()
assert a == b"0123\0\0\0\089abcdef", a
assert b == b"0123456789abcdef", b
PY

    touch "$mount_a/lock.txt" "$mount_b/lock.txt"
    flock -x "$mount_a/lock.txt" -c 'sleep 2' &
    local lock_pid=$!
    sleep 0.2
    if flock -n -x "$mount_a/lock.txt" -c true; then
        log "same-workspace lock did not conflict"
        wait "$lock_pid"
        return 1
    fi
    flock -n -x "$mount_b/lock.txt" -c true
    wait "$lock_pid"

    sync "$mount_a" "$mount_b"
    log "dual workspace isolation PASS"
}

base_revision() {
    local inspection
    inspection="$(run_cli inspect "$workspace_a")"
    python3 - "$inspection" <<'PY'
import json
import sys

revision = json.loads(sys.argv[1])["base_revision"]
if revision is None:
    raise SystemExit("workspace A has no fork base")
print(f"{revision['layer_id']}:{revision['sealed_version']}:{bytes(revision['root_hash']).hex()}")
PY
}

start_writers() {
    local label target ready deadline
    rm -f "$writer_stop_file" "$artifact_dir/writer-a.ready" "$artifact_dir/writer-b.ready"
    for label in a b; do
        if [[ "$label" == a ]]; then target="$mount_a"; else target="$mount_b"; fi
        (
            index=0
            while [[ ! -e "$writer_stop_file" ]]; do
                index=$((index + 1))
                printf '%s-%s\n' "$label" "$index" >"$target/concurrent-$label-$index.txt"
                : >"$artifact_dir/writer-$label.ready"
                sleep 0.02
            done
            sync "$target"
        ) >"$artifact_dir/writer-$label.log" 2>&1 &
        writer_pids+=("$!")
    done
    deadline=$((SECONDS + 30))
    for label in a b; do
        ready="$artifact_dir/writer-$label.ready"
        until [[ -e "$ready" ]]; do
            (( SECONDS < deadline )) || { log "writer $label did not start"; return 1; }
            sleep 0.1
        done
    done
}

wait_writers() {
    local pid status=0
    : >"$writer_stop_file"
    for pid in "${writer_pids[@]}"; do
        wait "$pid" || status=1
    done
    writer_pids=()
    (( status == 0 )) || log "concurrent mount writes failed; inspect writer logs"
    return "$status"
}

wait_cli_jobs() {
    local pid status=0
    for pid in "$@"; do
        wait "$pid" || status=1
    done
    (( status == 0 )) || log "concurrent CLI operations failed; inspect CLI logs"
    return "$status"
}

measure_cli() {
    local label="$1" index="$2" started ended status
    shift 2
    started="$(date +%s%N)"
    if run_cli "$@"; then status=0; else status=$?; fi
    ended="$(date +%s%N)"
    printf '{"start_ns":%s,"end_ns":%s,"exit_status":%s}\n' \
        "$started" "$ended" "$status" >"$artifact_dir/$label-$index.time.json"
    return "$status"
}

write_concurrency_metrics() {
    local label="$1" started="$2" ended="$3"
    python3 - "$artifact_dir" "$label" "$concurrency" "$started" "$ended" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
label = sys.argv[2]
count = int(sys.argv[3])
started = int(sys.argv[4])
ended = int(sys.argv[5])
wall_seconds = (ended - started) / 1_000_000_000
if wall_seconds <= 0:
    raise SystemExit("invalid batch wall duration")
operations = []
for index in range(1, count + 1):
    timing = json.loads((root / f"{label}-{index}.time.json").read_text())
    start_ns = timing["start_ns"]
    end_ns = timing["end_ns"]
    status = timing["exit_status"]
    if start_ns > end_ns or start_ns < started or end_ns > ended:
        raise SystemExit(f"invalid {label}-{index} timing")
    operations.append({
        "index": index,
        "exit_status": status,
        "latency_seconds": (end_ns - start_ns) / 1_000_000_000,
    })
succeeded = sum(operation["exit_status"] == 0 for operation in operations)
report = {
    "scenario": f"concurrent-{label}",
    "timed_scope": "parallel CLI operations, excluding workspace setup and writer drain",
    "mounted_writers": 2,
    "operation_count": count,
    "success_count": succeeded,
    "batch_wall_seconds": wall_seconds,
    "successful_operations_per_second": succeeded / wall_seconds,
    "operations": operations,
}
(root / f"concurrent-{label}-metrics.json").write_text(
    json.dumps(report, indent=2) + "\n", encoding="utf-8"
)
PY
}

verify_forks() {
    python3 - "$artifact_dir" "$concurrency" "$1" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
count = int(sys.argv[2])
expected = sys.argv[3]
seen = set()
for index in range(1, count + 1):
    records = json.loads((root / f"fork-{index}.json").read_text())
    if len(records) != 1:
        raise SystemExit(f"fork-{index} returned {len(records)} workspaces")
    record = records[0]
    revision = record["fork_base"]
    actual = f"{revision['layer_id']}:{revision['sealed_version']}:{bytes(revision['root_hash']).hex()}"
    if actual != expected:
        raise SystemExit(f"fork-{index} has an unexpected base revision")
    if record["workspace_id"] in seen:
        raise SystemExit(f"fork-{index} returned a duplicate workspace")
    seen.add(record["workspace_id"])
PY
}

concurrent_forks() {
    local revision index
    local -a jobs=()
    revision="$(base_revision)"
    start_writers
    local batch_started batch_ended
    batch_started="$(date +%s%N)"
    for index in $(seq 1 "$concurrency"); do
        measure_cli fork "$index" fork "$revision" --count 1 --owner "concurrent-fork-$index" \
            >"$artifact_dir/fork-$index.json" 2>"$artifact_dir/fork-$index.log" &
        jobs+=("$!")
    done
    local cli_status=0
    wait_cli_jobs "${jobs[@]}" || cli_status=1
    batch_ended="$(date +%s%N)"
    wait_writers
    write_concurrency_metrics fork "$batch_started" "$batch_ended"
    (( cli_status == 0 )) || return 1
    verify_forks "$revision"
    log "concurrent fork PASS ($concurrency distinct workspaces)"
}

concurrent_seals() {
    local revision index workspace
    local -a workspaces=() jobs=()
    revision="$(base_revision)"
    run_cli fork "$revision" --count "$concurrency" --owner concurrent-seal-setup \
        >"$artifact_dir/seal-workspaces.json" 2>"$artifact_dir/seal-setup.log"
    mapfile -t workspaces < <(python3 - "$artifact_dir/seal-workspaces.json" <<'PY'
import json
import sys

for record in json.load(open(sys.argv[1], encoding="utf-8")):
    print(record["workspace_id"])
PY
)
    [[ ${#workspaces[@]} -eq "$concurrency" ]] || {
        log "seal setup returned ${#workspaces[@]} workspaces, expected $concurrency"
        return 1
    }
    start_writers
    local batch_started batch_ended
    batch_started="$(date +%s%N)"
    index=0
    for workspace in "${workspaces[@]}"; do
        index=$((index + 1))
        measure_cli seal "$index" snapshot "$workspace" --name "concurrent-seal-$index" \
            --owner "concurrent-seal-$index" \
            >"$artifact_dir/seal-$index.json" 2>"$artifact_dir/seal-$index.log" &
        jobs+=("$!")
    done
    local cli_status=0
    wait_cli_jobs "${jobs[@]}" || cli_status=1
    batch_ended="$(date +%s%N)"
    wait_writers
    write_concurrency_metrics seal "$batch_started" "$batch_ended"
    (( cli_status == 0 )) || return 1
    python3 - "$artifact_dir" "$concurrency" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
count = int(sys.argv[2])
seen = set()
for index in range(1, count + 1):
    snapshot = json.loads((root / f"seal-{index}.json").read_text())
    revision = snapshot["revision"]
    identity = (revision["layer_id"], revision["sealed_version"])
    if identity in seen:
        raise SystemExit(f"seal-{index} returned a duplicate revision")
    seen.add(identity)
PY
    log "concurrent seal PASS ($concurrency distinct revisions)"
}

mkdir -p "$state_dir" "$data_dir" "$artifact_dir"
start_mount "$workspace_a" "$mount_a" "$state_dir/cache-a" "$artifact_dir/mount-a.log"
pid_a=$last_pid
wait_for_mount "$pid_a" "$mount_a" "$artifact_dir/mount-a.log"

case "$mode" in
    seed)
        seed_base
        ;;
    verify|concurrent-fork|concurrent-seal)
        [[ -n "$workspace_b" ]] || { log "BREWFS_WORKSPACE_B is required in $mode mode"; exit 2; }
        start_mount "$workspace_b" "$mount_b" "$state_dir/cache-b" "$artifact_dir/mount-b.log"
        pid_b=$last_pid
        wait_for_mount "$pid_b" "$mount_b" "$artifact_dir/mount-b.log"
        case "$mode" in
            verify) verify_isolation ;;
            concurrent-fork) concurrent_forks ;;
            concurrent-seal) concurrent_seals ;;
        esac
        ;;
    *)
        log "unsupported BREWFS_WORKSPACE_HARNESS_MODE: $mode"
        exit 2
        ;;
esac
