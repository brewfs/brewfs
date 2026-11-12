#!/usr/bin/env bash
# Run the ignored packed/005 real-backend lifecycle tests on this host
# against owned loopback Redis, PD/TiKV and RustFS services.
#
# Each invocation owns one Compose project, one artifact directory, unique
# metadata namespaces (chosen by each test), and one RustFS bucket set with
# freshly provisioned runtime/admin IAM identities. Results are recorded per
# case; any unknown failure, cleanup failure or incomplete resource journal
# makes the run non-zero. Existing artifacts are never overwritten.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.packed-lifecycle-backends.yml"
JOURNAL="$PROJECT_DIR/tools/perf/workspace_control_resource_journal.py"
IAM="$PROJECT_DIR/tools/testing/rustfs_owned_iam.py"

ARTIFACT_ROOT="${PACKED_LIFECYCLE_ARTIFACT_ROOT:?set PACKED_LIFECYCLE_ARTIFACT_ROOT}"
TEST_BIN="${PACKED_LIFECYCLE_TEST_BIN:?set PACKED_LIFECYCLE_TEST_BIN to the brewfs lib test executable}"
BREWFS_BIN="${PACKED_LIFECYCLE_BREWFS_BIN:?set PACKED_LIFECYCLE_BREWFS_BIN to the release brewfs executable}"
OBJECTS="${PACKED_LIFECYCLE_OBJECTS:-localfs}"
CASE_TIMEOUT="${PACKED_LIFECYCLE_CASE_TIMEOUT:-900}"
CASES_FILE="${1:?usage: $0 CASES_FILE (one exact test name per line)}"
DOCKER=(sudo -n docker)

log() { printf '[packed-lifecycle] %s\n' "$*"; }
die() { log "ERROR: $*" >&2; exit 2; }

case "$OBJECTS" in localfs|rustfs) ;; *) die "PACKED_LIFECYCLE_OBJECTS must be localfs or rustfs" ;; esac
[[ -x "$TEST_BIN" && -x "$BREWFS_BIN" && -r "$CASES_FILE" ]] || die "test binary, brewfs binary and cases file are required"
[[ -e /dev/fuse ]] || die "/dev/fuse is required"
[[ "$(id -u)" != 0 ]] || die "run as an unprivileged user; root bypasses object PUT faults"
if grep -qiE 'sqlite|sqlx' "$CASES_FILE"; then die "sqlite/sqlx metadata cases are out of scope"; fi

ts="$(date -u +%Y%m%dT%H%M%SZ)-$$"
run_id="packed-lifecycle-$OBJECTS-$ts"
run_dir="$ARTIFACT_ROOT/$run_id"
[[ ! -e "$run_dir" ]] || die "artifact directory already exists: $run_dir"
mkdir -p "$run_dir/cases"
project="brewfs-pl-$(printf '%s' "$ts" | tr 'A-Z' 'a-z' | tr -cd 'a-z0-9')"
journal="$run_dir/resource-journal.json"
owner="$(python3 -c 'import uuid;print(uuid.uuid4().hex)')"

# Ports derive from the PID so concurrent invocations never share a service.
base=$(( 20000 + ($$ % 400) * 100 ))
export PACKED_LIFECYCLE_REDIS_PORT=$((base + 1))
export PACKED_LIFECYCLE_PD_PORT=$((base + 2))
export PACKED_LIFECYCLE_PD_PEER_PORT=$((base + 3))
export PACKED_LIFECYCLE_TIKV_PORT=$((base + 4))
export PACKED_LIFECYCLE_TIKV_STATUS_PORT=$((base + 5))
export PACKED_LIFECYCLE_RUSTFS_PORT=$((base + 6))
export PACKED_LIFECYCLE_RUSTFS_ROOT_ACCESS_KEY="root$(python3 -c 'import secrets;print(secrets.token_hex(8))')"
export PACKED_LIFECYCLE_RUSTFS_ROOT_SECRET_KEY="$(python3 -c 'import secrets;print(secrets.token_hex(24))')"
if ! docker info >/dev/null 2>&1; then
    # The resource journal invokes plain `docker`; route it through sudo when
    # this user is not in the docker group, without modifying the helper.
    shim="$run_dir/.docker-shim"
    mkdir -p "$shim"
    printf '#!/bin/sh\nexec sudo -n %s "$@"\n' "$(command -v docker)" >"$shim/docker"
    chmod 700 "$shim/docker"
    export PATH="$shim:$PATH"
fi
# sudo drops the caller environment; pass Compose interpolation explicitly.
compose_env="$run_dir/.compose.env"
( umask 077; env | grep '^PACKED_LIFECYCLE_' >"$compose_env" )
compose=("${DOCKER[@]}" compose --env-file "$compose_env" -f "$COMPOSE_FILE" -p "$project")
profiles=(--profile redis --profile tikv)
[[ "$OBJECTS" == rustfs ]] && profiles+=(--profile rustfs)

python3 "$JOURNAL" init --path "$journal" --run-id "$run_id" --artifact "$run_dir" \
    --project "$project" --backend "redis+tikv+$OBJECTS" --namespace "per-test-uuid"

{
    echo "run_id=$run_id"
    echo "revision=$(git -C "$PROJECT_DIR" rev-parse HEAD)"
    echo "worktree_dirty=$(git -C "$PROJECT_DIR" status --porcelain | wc -l)"
    echo "objects=$OBJECTS"
    echo "case_timeout_s=$CASE_TIMEOUT"
    echo "compose_project=$project"
    echo "ports=redis:$PACKED_LIFECYCLE_REDIS_PORT,pd:$PACKED_LIFECYCLE_PD_PORT,tikv:$PACKED_LIFECYCLE_TIKV_PORT,rustfs:$PACKED_LIFECYCLE_RUSTFS_PORT"
} >"$run_dir/profile.env"
{
    printf '{"rustc":%s,"cargo":%s,"kernel":%s,"protoc":%s}\n' \
        "$(rustc -vV | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')" \
        "$(cargo --version | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read().strip()))')" \
        "$(uname -srv | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read().strip()))')" \
        "$(protoc --version 2>/dev/null | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read().strip()))')"
} >"$run_dir/toolchain.json"
sha256sum "$TEST_BIN" "$BREWFS_BIN" >"$run_dir/binary.sha256"
cp "$CASES_FILE" "$run_dir/cases.txt"
sha256sum "$run_dir/cases.txt" >"$run_dir/cases.sha256"

iam_env="$run_dir/.rustfs-iam.env"
cleanup_ran=0
cleanup() {
    local status=$?
    # INT/TERM exit through this handler, which then fires on EXIT again.
    (( cleanup_ran == 0 )) || exit "$status"
    cleanup_ran=1
    trap - EXIT INT TERM
    (( status != 0 )) || [[ -z "${received_signal:-}" ]] || status=130
    set +e
    python3 "$JOURNAL" stage --path "$journal" --stage cleanup --phase start
    local fuse_left
    fuse_left="$(grep -c " $run_dir/" /proc/mounts || true)"
    if [[ -n "${fault_pid:-}" ]]; then
        kill "$fault_pid" 2>/dev/null
        wait "$fault_pid" 2>/dev/null
    fi
    [[ -z "${fault_dir:-}" ]] || rm -rf "$fault_dir"
    if [[ "$OBJECTS" == rustfs && -f "$iam_env" ]]; then
        set -a; . "$iam_env"; set +a
        # Remove any leftover deny-PUT fault policy along with owned identities.
        export RUSTFS_OWNED_POLICIES="$RUSTFS_OWNED_POLICIES,brewfs-runtime-denyput-$owner"
        RUSTFS_ROOT_ACCESS_KEY="$PACKED_LIFECYCLE_RUSTFS_ROOT_ACCESS_KEY" \
        RUSTFS_ROOT_SECRET_KEY="$PACKED_LIFECYCLE_RUSTFS_ROOT_SECRET_KEY" \
            python3 "$IAM" remove --endpoint "http://127.0.0.1:$PACKED_LIFECYCLE_RUSTFS_PORT" \
            >"$run_dir/iam-cleanup.json" 2>&1
        local iam_status=$?
        rm -f "$iam_env"
    else
        local iam_status=0
    fi
    "${compose[@]}" "${profiles[@]}" logs --no-color >"$run_dir/backend-services.log" 2>&1
    "${compose[@]}" "${profiles[@]}" down -v --remove-orphans >"$run_dir/compose-down.log" 2>&1
    local down_status=$?
    rm -f "$compose_env"
    python3 "$JOURNAL" snapshot --path "$journal" --stage cleanup --phase cleanup --project "$project"
    local snap_status=$?
    python3 "$JOURNAL" check-empty --path "$journal"
    local empty_status=$?
    # Count only processes executing the test or brewfs binaries themselves
    # (by /proc/<pid>/exe), not shells whose arguments mention their paths.
    local test_procs=0 exe
    for exe in /proc/[0-9]*/exe; do
        case "$(readlink "$exe" 2>/dev/null)" in
            "$(readlink -f "$TEST_BIN")"|"$(readlink -f "$BREWFS_BIN")") test_procs=$((test_procs + 1)) ;;
        esac
    done
    {
        echo "fuse_mounts_under_artifact=$fuse_left"
        echo "test_processes_alive=$test_procs"
        echo "compose_down_exit=$down_status"
        echo "resource_snapshot_exit=$snap_status"
        echo "compose_resources_empty_exit=$empty_status"
        echo "iam_cleanup_exit=$iam_status"
    } >"$run_dir/cleanup-proof.txt"
    local cleanup_failed=0
    (( down_status == 0 && snap_status == 0 && empty_status == 0 && iam_status == 0 )) || cleanup_failed=1
    [[ "$fuse_left" == 0 && "$test_procs" == 0 ]] || cleanup_failed=1
    local final="$status"
    (( cleanup_failed == 0 || final != 0 )) || final=1
    python3 "$JOURNAL" stage --path "$journal" --stage cleanup --phase end --status "$cleanup_failed"
    python3 "$JOURNAL" finalize --path "$journal" --status "$final" --down-status "$down_status" || final=1
    python3 - "$run_dir" "$final" "$cleanup_failed" <<'PY'
import json, pathlib, sys
run = pathlib.Path(sys.argv[1]); final = int(sys.argv[2]); cleanup_failed = int(sys.argv[3])
rows = [json.loads(p.read_text()) for p in sorted((run / "cases").glob("*.json"))]
passed = sum(r["result"] == "passed" for r in rows)
expected = len([l for l in (run / "cases.txt").read_text().splitlines()
                if l.strip() and not l.lstrip().startswith("#")])
status = "passed" if final == 0 and passed == expected and not cleanup_failed else "rejected"
if len(rows) < expected and final != 0:
    status = "incomplete" if not cleanup_failed else "rejected"
if expected == 0 and status == "passed":
    # An empty case list proves only backend/IAM/cleanup plumbing.
    status = "incomplete"
summary = {"schema": "brewfs-packed-lifecycle-summary-v1", "status": status, "expected_cases": expected,
           "executed_cases": len(rows), "passed_cases": passed,
           "failed_cases": [r["case"] for r in rows if r["result"] != "passed"],
           "cleanup_failed": bool(cleanup_failed), "exit_status": final}
(run / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
manifest = json.loads((run / "run-manifest.json").read_text())
manifest["status"] = status
manifest["finalized"] = True
(run / "run-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
PY
    rm -rf "$run_dir/.docker-shim"
    log "status=$(python3 -c "import json;print(json.load(open('$run_dir/summary.json'))['status'])") artifacts=$run_dir"
    exit "$final"
}
trap cleanup EXIT
trap 'received_signal=1; exit 130' INT TERM

python3 - "$run_dir" "$run_id" "$project" "$OBJECTS" <<'PY'
import json, pathlib, sys, subprocess
run, run_id, project, objects = pathlib.Path(sys.argv[1]), *sys.argv[2:]
digest = {}
for image in ["docker.io/library/redis:7.2-alpine", "pingcap/pd:v8.5.0", "pingcap/tikv:v8.5.0",
              "w23geq7ncegg6etqsl.xuanyuan.run/rustfs/rustfs:latest"]:
    out = subprocess.run(["sudo", "-n", "docker", "image", "inspect", "--format", "{{json .RepoDigests}}", image],
                         capture_output=True, text=True, check=True).stdout
    digest[image] = json.loads(out)
manifest = {"schema": "brewfs-experiment-manifest-v1", "run_id": run_id, "scope": "packed-005-real-backend-lifecycle",
            "compose_project": project, "objects": objects, "metadata_backends": ["redis", "tikv"],
            "sqlite_sqlx": "excluded", "image_repo_digests": digest, "status": "running", "finalized": False}
(run / "run-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
PY

python3 "$JOURNAL" stage --path "$journal" --stage backend-start --phase start
start_backends() {
    local waited=(redis)
    [[ "$OBJECTS" == rustfs ]] && waited+=(rustfs)
    "${compose[@]}" "${profiles[@]}" up -d --wait "${waited[@]}" || return 1
    "${compose[@]}" "${profiles[@]}" up -d pd tikv || return 1
    local deadline=$((SECONDS + 180))
    until curl -fsS "http://127.0.0.1:$PACKED_LIFECYCLE_PD_PORT/pd/api/v1/stores" 2>/dev/null \
            | grep -Eq '"state_name"[[:space:]]*:[[:space:]]*"Up"'; do
        (( SECONDS < deadline )) || { log "TiKV store did not become Up"; return 1; }
        sleep 2
    done
}
if ! start_backends >"$run_dir/backend-start.log" 2>&1; then
    python3 "$JOURNAL" stage --path "$journal" --stage backend-start --phase end --status 1
    exit 1
fi
python3 "$JOURNAL" snapshot --path "$journal" --stage backend-start --phase end --project "$project"
python3 "$JOURNAL" stage --path "$journal" --stage backend-start --phase end --status 0

export BREWFS_TEST_REDIS_URL="redis://127.0.0.1:$PACKED_LIFECYCLE_REDIS_PORT/0"
export BREWFS_TEST_TIKV_PD_ENDPOINTS="127.0.0.1:$PACKED_LIFECYCLE_PD_PORT"
export BREWFS_TEST_BREWFS_BIN="$BREWFS_BIN"
if [[ "$OBJECTS" == rustfs ]]; then
    export BREWFS_TEST_ORIGINAL_OBJECT_BACKEND=rustfs
    export BREWFS_TEST_ORIGINAL_RUSTFS_ENDPOINT="http://127.0.0.1:$PACKED_LIFECYCLE_RUSTFS_PORT"
    export BREWFS_TEST_ORIGINAL_RUSTFS_OWNER="$owner"
    export BREWFS_TEST_ORIGINAL_RUSTFS_BUCKET="brewfs-v3-lifecycle-$owner"
    buckets=(--bucket "brewfs-v3-lifecycle-$owner")
    for case in rc tc rk tk rp tp; do buckets+=(--bucket "brewfs-v3-lifecycle-$owner-cli-$case"); done
    RUSTFS_ROOT_ACCESS_KEY="$PACKED_LIFECYCLE_RUSTFS_ROOT_ACCESS_KEY" \
    RUSTFS_ROOT_SECRET_KEY="$PACKED_LIFECYCLE_RUSTFS_ROOT_SECRET_KEY" \
        python3 "$IAM" provision --endpoint "$BREWFS_TEST_ORIGINAL_RUSTFS_ENDPOINT" \
        --owner "$owner" "${buckets[@]}" --env-file "$iam_env" >"$run_dir/iam-provision.json"
    set -a; . "$iam_env"; set +a
    # Owned IAM fault server for the pending-upload cases (deny/restore PUT).
    # AF_UNIX paths are limited to 108 bytes; keep the socket in a short
    # private directory owned by this run and removed during cleanup.
    fault_dir="$(mktemp -d /tmp/brewfs-pl-fault.XXXXXX)"
    chmod 700 "$fault_dir"
    export BREWFS_TEST_CLI_FAULT_SOCKET="$fault_dir/s"
    export BREWFS_TEST_CLI_FAULT_TOKEN="$(python3 -c 'import secrets;print(secrets.token_hex(16))')"
    RUSTFS_ROOT_ACCESS_KEY="$PACKED_LIFECYCLE_RUSTFS_ROOT_ACCESS_KEY" \
    RUSTFS_ROOT_SECRET_KEY="$PACKED_LIFECYCLE_RUSTFS_ROOT_SECRET_KEY" \
        python3 "$IAM" serve-fault --endpoint "$BREWFS_TEST_ORIGINAL_RUSTFS_ENDPOINT" --owner "$owner" \
        --socket "$BREWFS_TEST_CLI_FAULT_SOCKET" --event-log "$run_dir/iam-fault-events.jsonl" \
        >"$run_dir/iam-fault-server.log" 2>&1 &
    fault_pid=$!
    for _ in $(seq 50); do [[ -S "$BREWFS_TEST_CLI_FAULT_SOCKET" ]] && break; sleep 0.1; done
    [[ -S "$BREWFS_TEST_CLI_FAULT_SOCKET" ]] || { log "IAM fault server did not start"; exit 1; }
fi

status=0
while IFS= read -r case_name; do
    [[ -n "$case_name" && "$case_name" != \#* ]] || continue
    safe="$(printf '%s' "$case_name" | tr -c 'A-Za-z0-9_' '_')"
    python3 "$JOURNAL" stage --path "$journal" --stage "case:$safe" --phase start
    if [[ "$OBJECTS" == rustfs ]]; then
        case "$case_name" in
            *real_redis_cli_mount_mutation*) export BREWFS_TEST_CLI_RUSTFS_CASE=rc ;;
            *real_tikv_cli_mount_mutation*) export BREWFS_TEST_CLI_RUSTFS_CASE=tc ;;
            *real_redis_cli_kill_expiry*) export BREWFS_TEST_CLI_RUSTFS_CASE=rk ;;
            *real_tikv_cli_kill_expiry*) export BREWFS_TEST_CLI_RUSTFS_CASE=tk ;;
            *real_redis_cli_pending*) export BREWFS_TEST_CLI_RUSTFS_CASE=rp ;;
            *real_tikv_cli_pending*) export BREWFS_TEST_CLI_RUSTFS_CASE=tp ;;
            *) unset BREWFS_TEST_CLI_RUSTFS_CASE ;;
        esac
        [[ -z "${BREWFS_TEST_CLI_RUSTFS_CASE:-}" ]] \
            || export BREWFS_TEST_ORIGINAL_RUSTFS_BUCKET="brewfs-v3-lifecycle-$owner-cli-$BREWFS_TEST_CLI_RUSTFS_CASE"
        [[ -n "${BREWFS_TEST_CLI_RUSTFS_CASE:-}" ]] \
            || export BREWFS_TEST_ORIGINAL_RUSTFS_BUCKET="brewfs-v3-lifecycle-$owner"
    fi
    started_ns="$(date +%s%N)"
    set +e
    TMPDIR="$run_dir/tmp-$safe" timeout --kill-after=30 "$CASE_TIMEOUT" \
        bash -c 'mkdir -p "$TMPDIR" && exec "$@"' _ \
        "$TEST_BIN" --exact "$case_name" --ignored --nocapture --test-threads=1 \
        >"$run_dir/cases/$safe.log" 2>&1
    case_status=$?
    set -e
    ended_ns="$(date +%s%N)"
    tmp_left=0
    if [[ -d "$run_dir/tmp-$safe" ]]; then
        if grep -q " $run_dir/tmp-$safe" /proc/mounts; then tmp_left=1
        else rm -rf "$run_dir/tmp-$safe" || tmp_left=1; fi
    fi
    # --nocapture interleaves test stderr into libtest's "test ... ok" line,
    # so require exactly one started test plus libtest's own 1-passed tally.
    ran="$(grep -cE "^test $case_name \.\.\. " "$run_dir/cases/$safe.log" || true)"
    result=failed
    if (( case_status == 0 && ran == 1 && tmp_left == 0 )) \
            && grep -qE '^test result: ok\. 1 passed; 0 failed;' "$run_dir/cases/$safe.log"; then
        result=passed
    elif (( case_status == 124 || case_status == 137 )); then
        result=timeout
    fi
    python3 - "$run_dir/cases/$safe.json" "$case_name" "$result" "$case_status" "$started_ns" "$ended_ns" "$tmp_left" \
        "$run_dir/cases/$safe.log" <<'PY'
import hashlib, json, pathlib, sys
out, name, result, status, start, end, tmp_left, logp = sys.argv[1:]
log = pathlib.Path(logp).read_bytes()
pathlib.Path(out).write_text(json.dumps({"case": name, "result": result, "exit": int(status),
    "duration_s": (int(end) - int(start)) / 1e9, "scratch_left": tmp_left == "1",
    "log_sha256": hashlib.sha256(log).hexdigest(),
    "panic_excerpt": [l for l in log.decode(errors="replace").splitlines() if "panicked at" in l or "failed:" in l][:6]},
    indent=2) + "\n")
PY
    python3 "$JOURNAL" snapshot --path "$journal" --stage "case:$safe" --phase end --project "$project"
    python3 "$JOURNAL" stage --path "$journal" --stage "case:$safe" --phase end --status "$case_status"
    log "$case_name -> $result (exit $case_status)"
    [[ "$result" == passed ]] || status=1
done <"$CASES_FILE"
exit "$status"
