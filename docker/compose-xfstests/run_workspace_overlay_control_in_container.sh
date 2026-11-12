#!/usr/bin/env bash

set -euo pipefail

mode="${1:-}"
state_dir="${BREWFS_WORKSPACE_STATE_DIR:-/var/lib/brewfs}"
bin="${BREWFS_BIN:-/usr/local/bin/brewfs}"
catalog_backend="${BREWFS_WORKSPACE_META_BACKEND:-redis}"
catalog_url="${BREWFS_WORKSPACE_META_URL:-redis://redis:6379/0}"
catalog_tikv_endpoints="${BREWFS_WORKSPACE_TIKV_PD_ENDPOINTS:-pd:2379}"
catalog_namespace="${BREWFS_WORKSPACE_NAMESPACE:-brewfs-workspace}"
fixture="${BREWFS_PACKED_FIXTURE_BIN:-/usr/local/bin/packed_snapshot_fixture}"
data_dir="${BREWFS_DATA_DIR:-$state_dir/data}"

die() {
    printf '[workspace] ERROR: %s\n' "$*" >&2
    exit 1
}

[[ "$catalog_backend" == redis || "$catalog_backend" == tikv ]] \
    || die "BREWFS_WORKSPACE_META_BACKEND must be redis or tikv"
[[ "$mode" == init || "$mode" == fork || "$mode" == verify || "$mode" == concurrent-fork ]] \
    || die "usage: $0 {init|fork|verify|concurrent-fork}"

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

# The CLI may log before printing JSON; parse from the first top-level "{"/"[".
json_body() {
    python3 - "$1" <<'PY'
import sys

text = open(sys.argv[1], encoding="utf-8", errors="replace").read()
lines = text.splitlines()
start = next(i for i, line in enumerate(lines) if line in ("{", "["))
print("\n".join(lines[start:]))
PY
}

revision_of() {
    python3 - "$1" <<'PY'
import json
import sys

revision = json.load(open(sys.argv[1], encoding="utf-8"))["fork_base"]
print(f"{revision['layer_id']}:{revision['sealed_version']}:{bytes(revision['root_hash']).hex()}")
PY
}

# Shared seed content becomes the immutable packed lower. There is no seal:
# a mounted workspace never turns into a new lower.
build_seed_lower() {
    local seed="$state_dir/seed"
    rm -rf "$seed"
    mkdir -p "$seed" "$data_dir"
    printf 'shared-base\n' >"$seed/shared-base.txt"
    printf '0123456789abcdef' >"$seed/base-data.bin"
    python3 -c 'import sys; open(sys.argv[1], "wb").write(b"x" * 4096)' "$seed/mmap-base.bin"
    "$fixture" --wire-version 5 --source-directory "$seed" \
        --source-consistency best-effort-detected --source-hardlink-policy visible-links \
        --output-dir "$data_dir" --prefix workspace-lower \
        --manifest-output "$state_dir/manifest-key.txt" >"$state_dir/packed-fixture.log" 2>&1 \
        || { cat "$state_dir/packed-fixture.log" >&2; die "packed lower build failed"; }
    cat >"$state_dir/packed-data.yaml" <<YAML
data:
  backend: local-fs
  localfs:
    data_dir: $data_dir
YAML
}

case "$mode" in
    init)
        build_seed_lower
        run_cli init-packed-volume --manifest-key "$(tr -d '[:space:]' <"$state_dir/manifest-key.txt")" \
            --data-config "$state_dir/packed-data.yaml" --owner workspace-compose \
            >"$state_dir/workspace-init.raw"
        json_body "$state_dir/workspace-init.raw" >"$state_dir/workspace-init.json"
        workspace_id_from_json "$state_dir/workspace-init.json" \
            | head -n 1 >"$state_dir/root-workspace.id"
        revision_of "$state_dir/workspace-init.json" >"$state_dir/base-revision.txt"
        printf '[workspace] initialized root workspace %s over packed lower %s\n' \
            "$(tr -d '[:space:]' <"$state_dir/root-workspace.id")" \
            "$(tr -d '[:space:]' <"$state_dir/base-revision.txt")"
        ;;
    fork)
        revision="$(tr -d '[:space:]' <"$state_dir/base-revision.txt")"
        [[ -n "$revision" ]] || die "base revision is missing"
        run_cli fork "$revision" --count 2 --owner workspace-compose \
            >"$state_dir/workspace-fork.raw"
        json_body "$state_dir/workspace-fork.raw" >"$state_dir/workspace-fork.json"
        mapfile -t fork_ids < <(workspace_id_from_json "$state_dir/workspace-fork.json")
        [[ "${#fork_ids[@]}" -eq 2 ]] || die "expected two forked workspaces"
        printf '%s\n' "${fork_ids[0]}" >"$state_dir/workspace-a.id"
        printf '%s\n' "${fork_ids[1]}" >"$state_dir/workspace-b.id"
        printf '[workspace] forked workspaces %s and %s\n' \
            "${fork_ids[0]}" "${fork_ids[1]}"
        ;;
    verify|concurrent-fork)
        workspace_a="$(tr -d '[:space:]' <"$state_dir/workspace-a.id")"
        workspace_b="$(tr -d '[:space:]' <"$state_dir/workspace-b.id")"
        [[ -n "$workspace_a" && -n "$workspace_b" ]] \
            || die "forked workspace ids are missing"
        export BREWFS_WORKSPACE_HARNESS_MODE="$mode"
        export BREWFS_WORKSPACE_A="$workspace_a"
        export BREWFS_WORKSPACE_B="$workspace_b"
        export BREWFS_WORKSPACE_CATALOG_URL="$catalog_url"
        exec /usr/local/bin/run_workspace_dual_mount_in_container.sh
        ;;
esac
