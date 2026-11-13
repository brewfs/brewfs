#!/usr/bin/env bash
# Paired native-vs-packed metadata placement acceptance on this host.
#
# Both arms use the same release binary, fixture generator, file trace
# (scanner seed + layout), RustFS data path, FUSE/cache budgets and cleanup.
# Only metadata placement differs: "packed" reads packed objects, while
# "native-tikv" serves the same authenticated manifest from an owned TiKV
# namespace (import is a separate unmeasured step). Arms alternate inside each
# repetition so host drift affects both. A pair is valid only when both arms
# pass correctness, checksum and cleanup; otherwise the group is rejected.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_ROOT="${PAIRED_ARTIFACT_ROOT:?set PAIRED_ARTIFACT_ROOT}"
REPEATS="${PAIRED_REPEATS:-3}"
LABEL="${PAIRED_LABEL:-paired}"
ARMS="${PAIRED_ARMS:-packed native-tikv}"
for arm in $ARMS; do
    case "$arm" in packed|native-tikv) ;; *) echo "PAIRED_ARMS entries must be packed or native-tikv" >&2; exit 2 ;; esac
done
[[ "$REPEATS" =~ ^[1-9][0-9]*$ ]] || { echo "PAIRED_REPEATS must be positive" >&2; exit 2; }
[[ "$LABEL" =~ ^[a-z0-9-]+$ ]] || { echo "PAIRED_LABEL must be [a-z0-9-]" >&2; exit 2; }

ts="$(date -u +%Y%m%dT%H%M%SZ)-$$"
group="$OUT_ROOT/$LABEL-$ts"
[[ ! -e "$group" ]] || { echo "group directory exists" >&2; exit 2; }
mkdir -p "$group/runs"
project="brewfs-paired-$(printf '%s' "$ts" | tr 'A-Z' 'a-z' | tr -cd 'a-z0-9')"
base=$(( 30000 + ($$ % 300) * 10 ))
pd_port=$((base + 1)); pd_peer=$((base + 2)); tikv_port=$((base + 3)); tikv_status=$((base + 4))
env_file="$group/.compose.env"
( umask 077
  printf 'PACKED_LIFECYCLE_PD_PORT=%s\nPACKED_LIFECYCLE_PD_PEER_PORT=%s\nPACKED_LIFECYCLE_TIKV_PORT=%s\nPACKED_LIFECYCLE_TIKV_STATUS_PORT=%s\nPACKED_LIFECYCLE_REDIS_PORT=1\nPACKED_LIFECYCLE_RUSTFS_PORT=1\nPACKED_LIFECYCLE_RUSTFS_ROOT_ACCESS_KEY=unused\nPACKED_LIFECYCLE_RUSTFS_ROOT_SECRET_KEY=unused\n' \
    "$pd_port" "$pd_peer" "$tikv_port" "$tikv_status" >"$env_file" )
compose=(sudo -n docker compose --env-file "$env_file" -f "$ROOT/docker/compose-xfstests/docker-compose.packed-lifecycle-backends.yml" -p "$project" --profile tikv)
if ! docker info >/dev/null 2>&1; then
    # run_packed_local.sh calls plain docker and relies on Compose env
    # interpolation; route through sudo and keep only those variables.
    shim="$group/.docker-shim"
    mkdir -p "$shim"
    printf '#!/bin/sh\nexec sudo -n --preserve-env=RUSTFS_HOST_BIND,RUSTFS_S3_HOST_PORT,RUSTFS_CONSOLE_HOST_PORT,RUSTFS_ACCESS_KEY,RUSTFS_SECRET_KEY,BREWFS_S3_BUCKET,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY %s "$@"\n' \
        "$(command -v docker)" >"$shim/docker"
    chmod 700 "$shim/docker"
    export PATH="$shim:$PATH"
fi

cleanup() {
    local status=$?
    set +e
    "${compose[@]}" down -v --remove-orphans >"$group/tikv-down.log" 2>&1
    local down=$?
    local left
    left=$(sudo -n docker ps -aq --filter "label=com.docker.compose.project=$project" | wc -l)
    left=$(( left + $(sudo -n docker volume ls -q --filter "label=com.docker.compose.project=$project" | wc -l) ))
    rm -f "$env_file"
    rm -rf "$group/.docker-shim"
    printf 'tikv_down_exit=%s\ncompose_resources_left=%s\n' "$down" "$left" >"$group/cleanup-proof.txt"
    (( down == 0 && left == 0 )) || status=1
    python3 "$ROOT/tools/perf/packed_paired_summary.py" "$group" "$status"
    exit "$status"
}
child=""
stop() {
    # Forward the signal so the active run executes its own cleanup trap,
    # wait for it, then stop launching further runs.
    [[ -n "$child" ]] && kill -TERM "$child" 2>/dev/null && wait "$child" 2>/dev/null
    exit "$1"
}
trap cleanup EXIT
trap 'stop 130' INT
trap 'stop 143' TERM

python3 - "$group" "$REPEATS" "$@" <<'PY'
import json, os, pathlib, sys
group = pathlib.Path(sys.argv[1])
controls = {k: v for k, v in os.environ.items() if k.startswith("PACKED_LOCAL_") and k not in ("PACKED_LOCAL_ARM", "PACKED_LOCAL_TIKV_PD", "PACKED_LOCAL_ARTIFACT_ROOT")}
(group / "group.json").write_text(json.dumps({"repeats": int(sys.argv[2]), "arms": os.environ.get("PAIRED_ARMS", "packed native-tikv").split(), "controls": controls}, indent=2, sort_keys=True) + "\n")
PY

"${compose[@]}" up -d pd tikv >"$group/tikv-start.log" 2>&1
deadline=$((SECONDS + 180))
until curl -fsS "http://127.0.0.1:$pd_port/pd/api/v1/stores" 2>/dev/null | grep -Eq '"state_name"[[:space:]]*:[[:space:]]*"Up"'; do
    (( SECONDS < deadline )) || { echo "TiKV store did not become Up" >&2; exit 1; }
    sleep 2
done
sudo -n docker image inspect --format '{{json .RepoDigests}}' pingcap/pd:v8.5.0 pingcap/tikv:v8.5.0 rustfs/rustfs:latest \
    >"$group/image-digests.txt" 2>&1 || true

status=0
for rep in $(seq 1 "$REPEATS"); do
    # Alternate arm order per repetition to cancel ordering bias.
    read -r -a arms <<<"$ARMS"
    if (( rep % 2 == 1 )); then order=("${arms[@]}"); else order=(); for (( i=${#arms[@]}-1; i>=0; i-- )); do order+=("${arms[$i]}"); done; fi
    for arm in "${order[@]}"; do
        run="$group/runs/r$rep-$arm"
        mkdir -p "$run"
        pd=""
        [[ "$arm" == native-tikv ]] && pd="127.0.0.1:$pd_port"
        set +e
        PACKED_LOCAL_ARM="$arm" PACKED_LOCAL_TIKV_PD="$pd" PACKED_LOCAL_ARTIFACT_ROOT="$run" \
            bash "$ROOT/tools/perf/run_packed_local.sh" >"$run/runner.log" 2>&1 &
        child=$!
        wait "$child"
        rc=$?
        child=""
        set -e
        artifact="$(grep -oE 'artifact=[^ ]+' "$run/runner.log" | tail -1 | cut -d= -f2 || true)"
        printf '{"rep": %s, "arm": "%s", "exit": %s, "artifact": "%s"}\n' "$rep" "$arm" "$rc" "$artifact" >"$run/pair-entry.json"
        echo "rep=$rep arm=$arm exit=$rc"
        (( rc == 0 )) || status=1
    done
done
exit "$status"
