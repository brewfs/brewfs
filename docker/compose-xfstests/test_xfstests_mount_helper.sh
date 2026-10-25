#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmpdir="$(mktemp -d)"
cleanup_test() {
    if [[ -f "$tmpdir/pids" ]]; then
        while read -r pid; do
            kill "$pid" 2>/dev/null || true
        done <"$tmpdir/pids"
    fi
    wait 2>/dev/null || true
    rm -rf "$tmpdir"
}
trap cleanup_test EXIT

source "$SCRIPT_DIR/run_xfstests_in_container.sh"
export TEST_MOUNT_STATE="$tmpdir/state"
export TEST_MOUNT_PIDS="$tmpdir/pids"
export BREWFS_CONFIG_PATH="$tmpdir/config.yaml"
export BREWFS_MOUNT_WAIT_SECS=3
export BREWFS_PRE_MOUNT_WAIT_SECS=2
export BREWFS_MOUNT_READY_DELAY_SECS=0
mkdir -p "$TEST_MOUNT_STATE" "$tmpdir/mount with spaces"
touch "$BREWFS_CONFIG_PATH"

# Exercise the generated helper and real child lifetimes without FUSE or root.
# Replace mount observation, privileged diagnostics and the daemon executable.
findmnt() {
    [[ -f "$TEST_MOUNT_STATE/mounted" ]] || return 1
    printf 'fuse.brewfs\n'
}
setpriv() { :; }
export -f findmnt setpriv

cat >"$tmpdir/brewfs" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$$" >>"$TEST_MOUNT_PIDS"
if ! mkdir "$TEST_MOUNT_STATE/lease" 2>/dev/null; then
    echo 'Error: workspace is busy' >&2
    exit 1
fi
trap 'rm -f "$TEST_MOUNT_STATE/mounted"; rmdir "$TEST_MOUNT_STATE/lease"' EXIT
trap 'exit 0' TERM INT
if [[ -e "$TEST_MOUNT_STATE/fail-startup" ]]; then
    echo 'injected startup failure' >&2
    exit 7
fi
rm -f "$TEST_MOUNT_STATE/unmounted" "$TEST_MOUNT_STATE/release" "$TEST_MOUNT_STATE/draining"
touch "$TEST_MOUNT_STATE/mounted"
while [[ ! -e "$TEST_MOUNT_STATE/unmounted" ]]; do sleep 0.01; done
rm -f "$TEST_MOUNT_STATE/mounted"
touch "$TEST_MOUNT_STATE/draining"
while [[ ! -e "$TEST_MOUNT_STATE/release" ]]; do sleep 0.01; done
EOF
chmod +x "$tmpdir/brewfs"

wait_for() {
    local deadline=$((SECONDS + 5))
    until "$@"; do
        if (( SECONDS >= deadline )); then
            printf 'timed out waiting for: %s\n' "$*" >&2
            exit 1
        fi
        sleep 0.01
    done
}

for split_log in false true; do
    log_file="$tmpdir/brewfs-$split_log.log"
    fuse_log_file=""
    if [[ "$split_log" == true ]]; then
        fuse_log_file="$tmpdir/fuse.log"
    fi
    helper="$tmpdir/mount-helper"
    install_mount_helper "$helper"
    sed -i "s|/usr/local/bin/brewfs|$tmpdir/brewfs|g" "$helper"

    bash "$helper" brewfs "$tmpdir/mount with spaces"
    touch "$TEST_MOUNT_STATE/unmounted"
    wait_for test -f "$TEST_MOUNT_STATE/draining"
    # The kernel mount has disappeared, but the daemon still owns the lease.
    # A second helper must not launch another daemon until that owner exits.
    (
        sleep 0.5
        touch "$TEST_MOUNT_STATE/release"
    ) &
    release_pid=$!
    if ! bash "$helper" brewfs "$tmpdir/mount with spaces"; then
        cat "$log_file" >&2
        echo 'FAIL: remount raced the previous daemon teardown' >&2
        exit 1
    fi
    wait "$release_pid"
    [[ -f "$TEST_MOUNT_STATE/mounted" ]]

    touch "$TEST_MOUNT_STATE/unmounted"
    wait_for test -f "$TEST_MOUNT_STATE/draining"
    starts_before=$(wc -l <"$TEST_MOUNT_PIDS")
    if BREWFS_PRE_MOUNT_WAIT_SECS=0 bash "$helper" brewfs "$tmpdir/mount with spaces" >"$tmpdir/timeout.log" 2>&1; then
        echo 'FAIL: mount ignored a daemon still draining' >&2
        exit 1
    fi
    [[ $(wc -l <"$TEST_MOUNT_PIDS") == "$starts_before" ]]
    [[ ! -f "$TEST_MOUNT_STATE/mounted" ]]
    touch "$TEST_MOUNT_STATE/release"
    wait_for test ! -d "$TEST_MOUNT_STATE/lease"

    touch "$TEST_MOUNT_STATE/fail-startup"
    status=0
    bash "$helper" brewfs "$tmpdir/mount with spaces" >"$tmpdir/failure.log" 2>&1 || status=$?
    [[ "$status" == 7 ]]
    [[ ! -f "$TEST_MOUNT_STATE/mounted" && ! -d "$TEST_MOUNT_STATE/lease" ]]
    rm "$TEST_MOUNT_STATE/fail-startup"
    bash "$helper" brewfs "$tmpdir/mount with spaces"
    touch "$TEST_MOUNT_STATE/unmounted" "$TEST_MOUNT_STATE/release"
    wait_for test ! -d "$TEST_MOUNT_STATE/lease"
done

echo 'xfstests mount helper: remount, teardown timeout and startup failure passed'
