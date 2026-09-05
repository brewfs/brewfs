#!/usr/bin/env bash

set -euo pipefail

mode="${1:-}"
state_dir="${BREWFS_WORKSPACE_STATE_DIR:-/var/lib/brewfs}"
bin="${BREWFS_BIN:-/usr/local/bin/brewfs}"
catalog_backend="${BREWFS_WORKSPACE_META_BACKEND:-redis}"
catalog_url="${BREWFS_WORKSPACE_META_URL:-redis://redis:6379/0}"
catalog_tikv_endpoints="${BREWFS_WORKSPACE_TIKV_PD_ENDPOINTS:-pd:2379}"
catalog_namespace="${BREWFS_WORKSPACE_NAMESPACE:-brewfs-workspace-overlay}"

die() {
    printf '[workspace-overlay] ERROR: %s\n' "$*" >&2
    exit 1
}

[[ "$catalog_backend" == redis || "$catalog_backend" == tikv ]] \
    || die "BREWFS_WORKSPACE_META_BACKEND must be redis or tikv"
[[ "$mode" == init || "$mode" == seed || "$mode" == fork || "$mode" == verify ]] \
    || die "usage: $0 {init|seed|fork|verify}"

mkdir -p "$state_dir"

catalog_cli_args=(
    workspace
    --meta-backend "$catalog_backend"
    --workspace-namespace "$catalog_namespace"
)
case "$catalog_backend" in
    redis) catalog_cli_args+=(--meta-url "$catalog_url") ;;
    tikv) catalog_cli_args+=(--meta-tikv-pd-endpoints "$catalog_tikv_endpoints") ;;
esac

run_cli() {
    "$bin" "${catalog_cli_args[@]}" "$@"
}

workspace_id_from_json() {
    python3 - "$1" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    document = json.load(stream)
if isinstance(document, list):
    values = [item["workspace_id"] for item in document]
    print("\n".join(values))
else:
    print(document["workspace_id"])
PY
}

case "$mode" in
    init)
        run_cli init-volume --owner workspace-compose \
            >"$state_dir/workspace-init.json"
        workspace_id_from_json "$state_dir/workspace-init.json" \
            | head -n 1 >"$state_dir/root-workspace.id"
        printf '[workspace-overlay] initialized root workspace %s\n' \
            "$(tr -d '[:space:]' <"$state_dir/root-workspace.id")"
        ;;
    seed)
        root_workspace="$(tr -d '[:space:]' <"$state_dir/root-workspace.id")"
        [[ -n "$root_workspace" ]] || die "root workspace id is missing"
        export BREWFS_WORKSPACE_HARNESS_MODE=seed
        export BREWFS_WORKSPACE_A="$root_workspace"
        export BREWFS_WORKSPACE_CATALOG_URL="$catalog_url"
        unset BREWFS_WORKSPACE_B || true
        exec /usr/local/bin/run_workspace_overlay_dual_mount_in_container.sh
        ;;
    fork)
        root_workspace="$(tr -d '[:space:]' <"$state_dir/root-workspace.id")"
        [[ -n "$root_workspace" ]] || die "root workspace id is missing"
        run_cli fork "$root_workspace" --count 2 --owner workspace-compose \
            >"$state_dir/workspace-fork.json"
        mapfile -t fork_ids < <(workspace_id_from_json "$state_dir/workspace-fork.json")
        [[ "${#fork_ids[@]}" -eq 2 ]] || die "expected two forked workspaces"
        printf '%s\n' "${fork_ids[0]}" >"$state_dir/workspace-a.id"
        printf '%s\n' "${fork_ids[1]}" >"$state_dir/workspace-b.id"
        printf '[workspace-overlay] forked workspaces %s and %s\n' \
            "${fork_ids[0]}" "${fork_ids[1]}"
        ;;
    verify)
        workspace_a="$(tr -d '[:space:]' <"$state_dir/workspace-a.id")"
        workspace_b="$(tr -d '[:space:]' <"$state_dir/workspace-b.id")"
        [[ -n "$workspace_a" && -n "$workspace_b" ]] \
            || die "forked workspace ids are missing"
        export BREWFS_WORKSPACE_HARNESS_MODE=verify
        export BREWFS_WORKSPACE_A="$workspace_a"
        export BREWFS_WORKSPACE_B="$workspace_b"
        export BREWFS_WORKSPACE_CATALOG_URL="$catalog_url"
        exec /usr/local/bin/run_workspace_overlay_dual_mount_in_container.sh
        ;;
esac
