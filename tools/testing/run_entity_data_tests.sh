#!/usr/bin/env bash
# Functional entity-overlay gate against owned loopback Redis, optional TiKV,
# and real RustFS.
# No FUSE mount, release build, or performance workload is run.
set -Eeuo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT/docker/compose-xfstests/docker-compose.packed-lifecycle-backends.yml"

usage() {
    cat <<'EOF'
Usage: ENTITY_DATA_ARTIFACT_ROOT=/absolute/artifact/path bash tools/testing/run_entity_data_tests.sh

Requires Linux, Cargo, Python 3, Docker Compose v2, and Docker access directly
or through passwordless sudo. Creates only its own Redis/RustFS project and
volumes, then removes them on exit. Builds/tests run sequentially with four
Cargo jobs by default. No existing service or bucket is reused.

Optional: CARGO_BUILD_JOBS and PACKED_LIFECYCLE_REDIS_IMAGE/
PACKED_LIFECYCLE_RUSTFS_IMAGE. ENTITY_DATA_TIKV=true also starts isolated PD/TiKV
and includes entity transaction, fault-boundary and both-lower integration tests.
The default remains Redis only.
EOF
}

if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then usage; exit 0; fi
[[ $# == 0 ]] || { usage >&2; exit 2; }
: "${ENTITY_DATA_ARTIFACT_ROOT:?set ENTITY_DATA_ARTIFACT_ROOT to an artifact directory}"
[[ "$ENTITY_DATA_ARTIFACT_ROOT" == /* ]] || { echo "artifact root must be absolute" >&2; exit 2; }
[[ "$(uname -s)" == Linux ]] || { echo "loopback host-network runner requires Linux" >&2; exit 2; }
for required in cargo rustc python3 docker git setsid sha256sum; do
    command -v "$required" >/dev/null || { echo "missing command: $required" >&2; exit 2; }
done
DOCKER=(docker)
if ! docker info >/dev/null 2>&1; then
    sudo -n docker info >/dev/null 2>&1 || { echo "Docker requires direct access or passwordless sudo" >&2; exit 2; }
    DOCKER=(sudo -n docker)
fi
"${DOCKER[@]}" compose version >/dev/null

run_uuid="$(python3 -c 'import uuid; print(uuid.uuid4())')"
run_dir="$ENTITY_DATA_ARTIFACT_ROOT/entity-data-$(date -u +%Y%m%dT%H%M%SZ)-$run_uuid"
mkdir -p "$ENTITY_DATA_ARTIFACT_ROOT"
mkdir "$run_dir"
project="brewfs-entity-$run_uuid"
env_file="$run_dir/.compose.env"
test_pid=""
filters=(packed_entity_lower workspace_overlay::entity)
with_tikv="${ENTITY_DATA_TIKV:-false}"
[[ "$with_tikv" == true || "$with_tikv" == false ]] || { echo "ENTITY_DATA_TIKV must be true or false" >&2; exit 2; }
printf 'filter\tbucket\texit\tpassed\n' >"$run_dir/cases.tsv"

# Hold temporary sockets until all ports are selected, so they cannot be
# assigned the same port. A competing bind after release fails Compose startup;
# the gate never falls back to an existing host service.
read -r redis_port rustfs_port pd_port pd_peer_port tikv_port tikv_status_port < <(python3 - <<'PY'
import socket
sockets = [socket.socket() for _ in range(6)]
for sock in sockets:
    sock.bind(('127.0.0.1', 0))
print(*(sock.getsockname()[1] for sock in sockets))
PY
)
access="entity$(python3 -c 'import secrets; print(secrets.token_hex(8))')"
secret="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
printf '%s\n' \
    "PACKED_LIFECYCLE_REDIS_PORT=$redis_port" \
    "PACKED_LIFECYCLE_RUSTFS_PORT=$rustfs_port" \
    "PACKED_LIFECYCLE_REDIS_IMAGE=${PACKED_LIFECYCLE_REDIS_IMAGE:-docker.io/library/redis:7.2-alpine}" \
    "PACKED_LIFECYCLE_RUSTFS_IMAGE=${PACKED_LIFECYCLE_RUSTFS_IMAGE:-w23geq7ncegg6etqsl.xuanyuan.run/rustfs/rustfs:latest}" \
    "PACKED_LIFECYCLE_RUSTFS_ROOT_ACCESS_KEY=$access" \
    "PACKED_LIFECYCLE_RUSTFS_ROOT_SECRET_KEY=$secret" \
    "PACKED_LIFECYCLE_PD_PORT=$pd_port" "PACKED_LIFECYCLE_PD_PEER_PORT=$pd_peer_port" \
    "PACKED_LIFECYCLE_TIKV_PORT=$tikv_port" "PACKED_LIFECYCLE_TIKV_STATUS_PORT=$tikv_status_port" >"$env_file"
# Compose gives ambient variables precedence over --env-file, including under
# direct Docker access. Remove only this runner's interpolation names so it
# always uses its newly selected ports and credentials.
unset PACKED_LIFECYCLE_REDIS_PORT PACKED_LIFECYCLE_RUSTFS_PORT \
    PACKED_LIFECYCLE_REDIS_IMAGE PACKED_LIFECYCLE_RUSTFS_IMAGE \
    PACKED_LIFECYCLE_RUSTFS_ROOT_ACCESS_KEY PACKED_LIFECYCLE_RUSTFS_ROOT_SECRET_KEY \
    PACKED_LIFECYCLE_PD_PORT PACKED_LIFECYCLE_PD_PEER_PORT \
    PACKED_LIFECYCLE_TIKV_PORT PACKED_LIFECYCLE_TIKV_STATUS_PORT
compose=("${DOCKER[@]}" compose --env-file "$env_file" -f "$COMPOSE_FILE" \
    -p "$project" --profile redis --profile rustfs)
if [[ "$with_tikv" == true ]]; then compose+=(--profile tikv); fi

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    set +e
    if [[ -n "$test_pid" ]]; then
        kill -TERM -- "-$test_pid" 2>/dev/null
        for _ in {1..30}; do
            kill -0 -- "-$test_pid" 2>/dev/null || break
            sleep 1
        done
        kill -KILL -- "-$test_pid" 2>/dev/null
        wait "$test_pid" 2>/dev/null
    fi
    "${compose[@]}" logs --no-color >"$run_dir/backend-services.log" 2>&1
    "${compose[@]}" down --timeout 30 -v --remove-orphans >"$run_dir/compose-down.log" 2>&1
    local down_status=$? containers volumes container_status volume_status
    containers="$("${DOCKER[@]}" ps -aq --filter "label=com.docker.compose.project=$project" 2>"$run_dir/container-query.log")"
    container_status=$?
    volumes="$("${DOCKER[@]}" volume ls -q --filter "label=com.docker.compose.project=$project" 2>"$run_dir/volume-query.log")"
    volume_status=$?
    printf 'compose_down_exit=%s\ncontainer_query_exit=%s\nvolume_query_exit=%s\ncontainers_left=%s\nvolumes_left=%s\n' \
        "$down_status" "$container_status" "$volume_status" "$containers" "$volumes" >"$run_dir/cleanup-proof.txt"
    if (( down_status != 0 || container_status != 0 || volume_status != 0 )) || [[ -n "$containers$volumes" ]]; then
        status=1
    fi
    rm -f "$env_file"
    python3 - "$run_dir" "$status" <<'PY'
import csv, json, pathlib, sys
run = pathlib.Path(sys.argv[1])
status = int(sys.argv[2])
with (run / 'cases.tsv').open() as source:
    rows = list(csv.DictReader(source, delimiter='\t'))
complete = len(rows) == 2 and all(int(row['exit']) == 0 and int(row['passed']) > 0 for row in rows)
result = 'passed' if status == 0 and complete else 'incomplete' if len(rows) < 2 else 'failed'
(run / 'summary.json').write_text(json.dumps({'status': result, 'exit': status, 'cases': rows}, indent=2) + '\n')
print(f'entity-data: {result}; artifacts={run}')
sys.exit(0 if status == 0 and complete else 1)
PY
    local summary_status=$?
    (( summary_status == 0 )) || { (( status != 0 )) || status=1; }
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

export BREWFS_TEST_REDIS_URL="redis://127.0.0.1:$redis_port/0"
export BREWFS_PACKED_GRAPH_TEST_S3_ENDPOINT="http://127.0.0.1:$rustfs_port"
export AWS_ACCESS_KEY_ID="$access" AWS_SECRET_ACCESS_KEY="$secret"
export AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
unset AWS_SESSION_TOKEN AWS_PROFILE AWS_DEFAULT_PROFILE
export HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy=
export NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}" CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_TERM_COLOR=never
cd "$ROOT"
{
    printf 'revision=%s\nproject=%s\nredis_port=%s\nrustfs_port=%s\ncargo_jobs=%s\n' \
        "$(git rev-parse HEAD)" "$project" "$redis_port" "$rustfs_port" "$CARGO_BUILD_JOBS"
    cargo --version
    rustc -vV
} >"$run_dir/provenance.txt"
git status --short >"$run_dir/worktree-status.txt"
git diff HEAD --binary >"$run_dir/tracked-worktree.patch"
python3 - "$run_dir/rust-source-sha256.json" <<'PY'
import hashlib, json, pathlib, subprocess, sys
names = subprocess.check_output([
    'git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard', '--', '*.rs'
])
files = []
for name in sorted(set(names.decode().split('\0')) - {''}):
    path = pathlib.Path(name)
    files.append({'path': name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()
                  if path.is_file() else None})
pathlib.Path(sys.argv[1]).write_text(json.dumps({
    'scope': 'tracked and nonignored untracked Rust files; null means absent',
    'files': files,
}, indent=2) + '\n')
PY
sha256sum "$ROOT/tools/testing/run_entity_data_tests.sh" "$COMPOSE_FILE" >"$run_dir/runner.sha256"
printf '[entity-data] starting isolated services; artifacts=%s\n' "$run_dir"
"${compose[@]}" up -d --wait --wait-timeout 180 redis rustfs >"$run_dir/backend-start.log" 2>&1
if [[ "$with_tikv" == true ]]; then
    "${compose[@]}" up -d pd tikv >>"$run_dir/backend-start.log" 2>&1
    python3 "$ROOT/tools/testing/wait_entity_tikv.py" "$pd_port"
    export BREWFS_TEST_TIKV_PD_ENDPOINTS="127.0.0.1:$pd_port"
    export BREWFS_TEST_TIKV_STATUS_ENDPOINT="http://127.0.0.1:$tikv_status_port"
else
    unset BREWFS_TEST_TIKV_PD_ENDPOINTS BREWFS_TEST_TIKV_STATUS_ENDPOINT
fi
service_ids="$("${compose[@]}" ps -q)"
[[ -n "$service_ids" ]] || { echo "Compose started no backend containers" >&2; exit 1; }
while IFS= read -r container; do
    "${DOCKER[@]}" inspect --format '{{.Name}} {{.Image}}' "$container"
done <<<"$service_ids" >"$run_dir/backend-image-ids.txt"

for filter in "${filters[@]}"; do
    export BREWFS_PACKED_GRAPH_TEST_S3_BUCKET="brewfs-graph-test-$(python3 -c 'import uuid; print(uuid.uuid4())')"
    slug="${filter//::/-}"
    # Match the mandatory workspace test target/feature graph. Selecting only
    # brewfs omits brewfs-stats -> crossterm -> mio default/log features and
    # produces a second full library test executable. Other targets have no
    # matching cases; the parser below permits their empty suite summaries.
    command=(cargo test --workspace --features workspace-overlay --lib --bins "$filter" -- --include-ignored --test-threads=1)
    if [[ "$with_tikv" != true ]]; then command+=(--skip tikv_entity); fi
    printf '%q ' "${command[@]}" >>"$run_dir/commands.log"
    printf '\n' >>"$run_dir/commands.log"
    printf '[entity-data] running %s; log=%s/%s.log\n' "$filter" "$run_dir" "$slug"
    setsid "${command[@]}" >"$run_dir/$slug.log" 2>&1 &
    test_pid=$!
    if wait "$test_pid"; then result=0; else result=$?; fi
    test_pid=""
    passed=0
    if (( result == 0 )); then
        if passed="$(python3 - "$run_dir/$slug.log" <<'PY'
import pathlib, re, sys
text = pathlib.Path(sys.argv[1]).read_text()
summaries = re.findall(r'^test result:.*$', text, re.MULTILINE)
matches = re.findall(
    r'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; '
    r'\d+ measured; \d+ filtered out; finished in \d+(?:\.\d+)?s$', text, re.MULTILINE,
)
positive = [int(passed) for _, passed, _, _ in matches if int(passed) > 0]
if (len(matches) != len(summaries) or len(positive) != 1
        or any(status != 'ok' or int(failed) or int(ignored)
               for status, _, failed, ignored in matches)):
    sys.exit('expected one nonempty successful suite and only empty successful companions, with no failed or ignored tests')
print(positive[0])
PY
)"; then :; else result=1; passed=0; fi
    fi
    printf '%s\t%s\t%s\t%s\n' "$filter" "$BREWFS_PACKED_GRAPH_TEST_S3_BUCKET" "$result" "$passed" >>"$run_dir/cases.tsv"
    if (( result != 0 )); then
        tail -n 60 "$run_dir/$slug.log" >&2
        exit "$result"
    fi
    printf '[entity-data] %s: %s passed\n' "$filter" "$passed"
done
