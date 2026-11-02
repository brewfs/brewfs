#!/usr/bin/env bash
set -uo pipefail
mkdir -p "$BREWFS_ARTIFACT_DIR"
fusermount3() {
    command fusermount3 "$@"
    local status=$?
    printf '%s fusermount3 status=%s args=' "$(date -u +%FT%TZ)" "$status" >> "$BREWFS_ARTIFACT_DIR/unmount-commands.log"
    printf '%q ' "$@" >> "$BREWFS_ARTIFACT_DIR/unmount-commands.log"
    printf '\n' >> "$BREWFS_ARTIFACT_DIR/unmount-commands.log"
    return "$status"
}
export -f fusermount3
bash /usr/local/bin/run_perf_in_container.sh
runner_status=$?
findmnt -rn -o TARGET,FSTYPE > "$BREWFS_ARTIFACT_DIR/mounts-after-runner.txt"
ps -eo pid,ppid,stat,args > "$BREWFS_ARTIFACT_DIR/processes-after-runner.txt"
printf '%s\n' "$runner_status" > "$BREWFS_ARTIFACT_DIR/runner-exit.status"
exit "$runner_status"
