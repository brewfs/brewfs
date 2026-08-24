#!/usr/bin/env bash

# Run one filesystem suite against both forked workspaces.  The wrapper is
# intentionally image-local so Compose can run it without mounting host
# scripts into a privileged FUSE container.
set -uo pipefail

suite="${WORKSPACE_OVERLAY_SUITE:?WORKSPACE_OVERLAY_SUITE is required}"
state_dir="${BREWFS_WORKSPACE_STATE_DIR:-/var/lib/brewfs}"
artifact_root="${BREWFS_ARTIFACT_ROOT:-/artifacts}"

case "$suite" in
    xfstests)
        suite_runner=/usr/local/bin/run_xfstests_in_container.sh
        ;;
    ltp)
        suite_runner=/usr/local/bin/run_ltp_in_container.sh
        ;;
    *)
        printf 'unsupported workspace overlay suite: %s\n' "$suite" >&2
        exit 2
        ;;
esac

status=0
for workspace_label in a b; do
    workspace_file="$state_dir/workspace-${workspace_label}.id"
    if [[ ! -s "$workspace_file" ]]; then
        printf 'missing workspace id: %s\n' "$workspace_file" >&2
        status=2
        continue
    fi

    workspace_id="$(tr -d '[:space:]' <"$workspace_file")"
    artifact_dir="$artifact_root/workspace-${workspace_label}/${suite}"
    mkdir -p "$artifact_dir"

    printf '[workspace-overlay] running %s on workspace %s (%s)\n' \
        "$suite" "$workspace_label" "$workspace_id"
    set +e
    env \
        BREWFS_VOLUME_FORMAT=workspace-v1 \
        BREWFS_WORKSPACE_ID="$workspace_id" \
        BREWFS_ARTIFACT_DIR="$artifact_dir" \
        BREWFS_ARTIFACT_ROOT="$artifact_root" \
        BREWFS_WORKSPACE_STATE_DIR="$state_dir" \
        bash "$suite_runner"
    suite_status=$?
    set -e
    if (( suite_status != 0 )); then
        printf '[workspace-overlay] %s failed on workspace %s (exit=%s)\n' \
            "$suite" "$workspace_label" "$suite_status" >&2
        status=1
    fi
done

exit "$status"
