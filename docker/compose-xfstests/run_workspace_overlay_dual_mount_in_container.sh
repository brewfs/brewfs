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
    local -a catalog_args=(
        --meta-backend "$catalog_backend"
        --workspace-namespace "$catalog_namespace"
    )
    case "$catalog_backend" in
        sqlx|redis)
            catalog_args+=(--meta-url "$catalog_url")
            ;;
        tikv)
            catalog_args+=(--meta-tikv-pd-endpoints "$catalog_tikv_endpoints")
            ;;
        *)
            log "unsupported workspace catalog backend: $catalog_backend"
            return 2
            ;;
    esac

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

mkdir -p "$state_dir" "$data_dir" "$artifact_dir"
start_mount "$workspace_a" "$mount_a" "$state_dir/cache-a" "$artifact_dir/mount-a.log"
pid_a=$last_pid
wait_for_mount "$pid_a" "$mount_a" "$artifact_dir/mount-a.log"

case "$mode" in
    seed)
        seed_base
        ;;
    verify)
        start_mount "$workspace_b" "$mount_b" "$state_dir/cache-b" "$artifact_dir/mount-b.log"
        pid_b=$last_pid
        wait_for_mount "$pid_b" "$mount_b" "$artifact_dir/mount-b.log"
        verify_isolation
        ;;
    *)
        log "unsupported BREWFS_WORKSPACE_HARNESS_MODE: $mode"
        exit 2
        ;;
esac
