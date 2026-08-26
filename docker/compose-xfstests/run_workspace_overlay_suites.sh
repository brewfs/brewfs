#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
ARTIFACT_ROOT="${BREWFS_WORKSPACE_ARTIFACT_ROOT:-$SCRIPT_DIR/artifacts}"
XFSTESTS_IMAGE="${BREWFS_WORKSPACE_XFSTESTS_IMAGE:-brewfs-workspace-xfstests:local}"
PJDFSTEST_IMAGE="${BREWFS_WORKSPACE_PJDFSTEST_IMAGE:-brewfs-workspace-pjdfstest:local}"
FALLBACK_BASE_IMAGE="${BREWFS_WORKSPACE_FALLBACK_BASE_IMAGE:-slayerfs-xfstests:local}"
XFSTESTS_CASES="generic/001 generic/002 generic/100"
RUN_XFSTESTS=true
RUN_PJDFSTEST=true
BUILD_IMAGES=true
PARALLEL_SUITES=false
META_BACKEND="${BREWFS_WORKSPACE_META_BACKEND:-sqlite}"
WORKSPACE_COUNT=2

log() { printf '[workspace-overlay] %s\n' "$*"; }
die() { log "ERROR: $*" >&2; exit 2; }

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Build the feature-enabled BrewFS test images, create two workspaces from one
sealed base, verify simultaneous FUSE isolation, and run each selected POSIX
suite against both workspaces.

Options:
  --xfstests-cases "CASES"  xfstests cases (default: $XFSTESTS_CASES)
  --skip-xfstests           do not run xfstests
  --skip-pjdfstest          do not run pjdfstest
  --reuse-images            reuse existing feature-enabled images
  --parallel-suites         run workspace A and B suites concurrently
  --single-workspace        run suites only on workspace A
  --meta-backend BACKEND    workspace catalog: sqlite, redis or tikv
  -h, --help                show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --xfstests-cases)
            [[ $# -ge 2 ]] || die "--xfstests-cases requires a value"
            XFSTESTS_CASES="$2"
            shift 2
            ;;
        --skip-xfstests)
            RUN_XFSTESTS=false
            shift
            ;;
        --skip-pjdfstest)
            RUN_PJDFSTEST=false
            shift
            ;;
        --reuse-images)
            BUILD_IMAGES=false
            shift
            ;;
        --parallel-suites)
            PARALLEL_SUITES=true
            shift
            ;;
        --single-workspace)
            WORKSPACE_COUNT=1
            shift
            ;;
        --meta-backend)
            [[ $# -ge 2 ]] || die "--meta-backend requires a value"
            META_BACKEND="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown option: $1"
            ;;
    esac
done

case "$META_BACKEND" in
    sqlite|redis|tikv) ;;
    *) die "--meta-backend must be sqlite, redis or tikv" ;;
esac

command -v cargo >/dev/null || die "cargo is required"
command -v docker >/dev/null || die "docker is required"
command -v python3 >/dev/null || die "python3 is required"
[[ -e /dev/fuse ]] || die "/dev/fuse is required"

ts="$(date +%Y%m%d-%H%M%S)-$RANDOM"
run_dir="$ARTIFACT_ROOT/workspace-overlay-$ts"
state_dir="$run_dir/state"
mkdir -p "$state_dir" "$run_dir/dual-mount" "$run_dir/xfstests-a" \
    "$run_dir/xfstests-b" "$run_dir/pjdfstest-a" "$run_dir/pjdfstest-b"

network="brewfs-workspace-$ts"
dependency_containers=()
catalog_namespace="workspace-$ts"
catalog_url="sqlite:///var/lib/brewfs/workspace.db?mode=rwc"
catalog_cli_backend="sqlx"
catalog_tikv_endpoints="pd:2379"
harness_script="$SCRIPT_DIR/run_workspace_overlay_dual_mount_in_container.sh"

cleanup_dependencies() {
    local status=$?
    set +e
    for container in "${dependency_containers[@]}"; do
        docker rm -f "$container" >/dev/null 2>&1 || true
    done
    docker network rm "$network" >/dev/null 2>&1 || true
    exit "$status"
}
trap cleanup_dependencies EXIT INT TERM

docker network create "$network" >/dev/null
case "$META_BACKEND" in
    sqlite)
        ;;
    redis)
        catalog_cli_backend="redis"
        catalog_url="redis://redis:6379"
        redis_container="brewfs-workspace-redis-$ts"
        dependency_containers+=("$redis_container")
        log "starting Redis workspace catalog"
        docker run --rm -d --name "$redis_container" --network "$network" \
            --network-alias redis redis:7.2-alpine \
            redis-server --save '' --appendonly no >/dev/null
        for _ in $(seq 1 60); do
            docker exec "$redis_container" redis-cli ping 2>/dev/null | grep -q PONG && break
            sleep 1
        done
        docker exec "$redis_container" redis-cli ping 2>/dev/null | grep -q PONG \
            || die "Redis did not become ready"
        ;;
    tikv)
        catalog_cli_backend="tikv"
        pd_container="brewfs-workspace-pd-$ts"
        tikv_container="brewfs-workspace-tikv-$ts"
        dependency_containers+=("$tikv_container" "$pd_container")
        log "starting TiKV workspace catalog"
        docker run --rm -d --name "$pd_container" --network "$network" \
            --network-alias pd pingcap/pd:v8.5.0 \
            --name=pd --data-dir=/data/pd \
            --client-urls=http://0.0.0.0:2379 \
            --advertise-client-urls=http://pd:2379 \
            --peer-urls=http://0.0.0.0:2380 \
            --advertise-peer-urls=http://pd:2380 \
            --initial-cluster=pd=http://pd:2380 --log-level=warn >/dev/null
        docker run --rm -d --name "$tikv_container" --network "$network" \
            --network-alias tikv pingcap/tikv:v8.5.0 \
            --addr=0.0.0.0:20160 --advertise-addr=tikv:20160 \
            --status-addr=0.0.0.0:20180 --pd=pd:2379 \
            --data-dir=/data/tikv --log-level=warn >/dev/null
        for _ in $(seq 1 180); do
            body="$(docker run --rm --network "$network" curlimages/curl:8.10.1 \
                -fsS http://pd:2379/pd/api/v1/stores 2>/dev/null || true)"
            grep -Eq '"state_name"[[:space:]]*:[[:space:]]*"Up"' <<<"$body" && break
            sleep 1
        done
        grep -Eq '"state_name"[[:space:]]*:[[:space:]]*"Up"' <<<"${body:-}" \
            || die "TiKV did not become ready"
        ;;
esac

catalog_cli_args=(
    workspace
    --meta-backend "$catalog_cli_backend"
    --workspace-namespace "$catalog_namespace"
)
case "$catalog_cli_backend" in
    sqlx|redis) catalog_cli_args+=(--meta-url "$catalog_url") ;;
    tikv) catalog_cli_args+=(--meta-tikv-pd-endpoints "$catalog_tikv_endpoints") ;;
esac

if [[ "$BUILD_IMAGES" == true ]]; then
    log "building feature-enabled release binary"
    (
        cd "$PROJECT_DIR"
        CARGO_PROFILE_RELEASE_DEBUG=0 cargo build --release -p brewfs --bin brewfs \
            --features workspace-overlay
        mkdir -p target/docker
        install -m 755 target/release/brewfs target/docker/brewfs
    )
    log "building xfstests image $XFSTESTS_IMAGE"
    if docker image inspect "$FALLBACK_BASE_IMAGE" >/dev/null 2>&1; then
        docker build --pull=false \
            --build-arg BASE_IMAGE="$FALLBACK_BASE_IMAGE" \
            -t "$XFSTESTS_IMAGE" \
            -f "$SCRIPT_DIR/Dockerfile.workspace-overlay-xfstests" "$PROJECT_DIR"
    else
        docker build -t "$XFSTESTS_IMAGE" -f "$SCRIPT_DIR/Dockerfile" "$PROJECT_DIR"
    fi
    if [[ "$RUN_PJDFSTEST" == true ]]; then
        log "building pjdfstest image $PJDFSTEST_IMAGE"
        if docker image inspect "$FALLBACK_BASE_IMAGE" >/dev/null 2>&1; then
            docker build --pull=false \
                --build-arg BASE_IMAGE="$FALLBACK_BASE_IMAGE" \
                -t "$PJDFSTEST_IMAGE" \
                -f "$SCRIPT_DIR/Dockerfile.workspace-overlay-pjdfstest" "$PROJECT_DIR"
        else
            docker build -t "$PJDFSTEST_IMAGE" -f "$SCRIPT_DIR/Dockerfile.pjdfstest" "$PROJECT_DIR"
        fi
    fi
fi

run_cli() {
    docker run --rm \
        --network "$network" \
        --workdir /var/lib/brewfs \
        -v "$state_dir:/var/lib/brewfs" \
        --entrypoint /usr/local/bin/brewfs \
        "$XFSTESTS_IMAGE" "$@"
}

extract_workspace_id() {
    python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["workspace_id"])' "$1"
}

extract_fork_ids() {
    python3 -c 'import json,sys; print("\n".join(item["workspace_id"] for item in json.load(open(sys.argv[1]))))' "$1"
}

run_dual_harness() {
    local mode="$1"
    local workspace_a="$2"
    local workspace_b="${3:-}"
    local artifacts="$4"
    docker run --rm \
        --network "$network" \
        --privileged \
        --device /dev/fuse:/dev/fuse \
        --security-opt apparmor=unconfined \
        --workdir /var/lib/brewfs \
        -v "$state_dir:/var/lib/brewfs" \
        -v "$artifacts:/artifacts" \
        -v "$harness_script:/workspace-harness.sh:ro" \
        -e BREWFS_WORKSPACE_HARNESS_MODE="$mode" \
        -e BREWFS_WORKSPACE_A="$workspace_a" \
        -e BREWFS_WORKSPACE_B="$workspace_b" \
        -e BREWFS_WORKSPACE_CATALOG_URL="$catalog_url" \
        -e BREWFS_WORKSPACE_META_BACKEND="$catalog_cli_backend" \
        -e BREWFS_WORKSPACE_TIKV_PD_ENDPOINTS="$catalog_tikv_endpoints" \
        -e BREWFS_WORKSPACE_NAMESPACE="$catalog_namespace" \
        -e BREWFS_FUSE_OP_LOG="${BREWFS_FUSE_OP_LOG:-0}" \
        -e RUST_LOG="${RUST_LOG:-brewfs=info,asyncfuse=warn}" \
        --entrypoint /bin/bash \
        "$XFSTESTS_IMAGE" /workspace-harness.sh
}

run_suite() {
    local image="$1"
    local workspace="$2"
    local artifacts="$3"
    shift 3
    # xfstests includes mmap timestamp checks (generic/080, generic/215).
    # Enable FUSE writeback for the test mount so mmap dirty pages reach the
    # filesystem release/flush path where mtime/ctime are persisted. Keep it
    # overridable for debugging and performance comparisons.
    docker run --rm \
        --network "$network" \
        --privileged \
        --device /dev/fuse:/dev/fuse \
        --security-opt apparmor=unconfined \
        --workdir /var/lib/brewfs \
        -v "$state_dir:/var/lib/brewfs" \
        -v "$artifacts:/artifacts" \
        -e BREWFS_VOLUME_FORMAT=workspace-v1 \
        -e BREWFS_WORKSPACE_ID="$workspace" \
        -e BREWFS_META_BACKEND="$META_BACKEND" \
        -e BREWFS_META_URL="$catalog_url" \
        -e BREWFS_META_TIKV_PD_ENDPOINTS="$catalog_tikv_endpoints" \
        -e BREWFS_META_TIKV_NAMESPACE="$catalog_namespace" \
        -e BREWFS_WORKSPACE_NAMESPACE="$catalog_namespace" \
        -e BREWFS_SQLITE_PATH=/var/lib/brewfs/workspace.db \
        -e BREWFS_DATA_BACKEND=local-fs \
        -e BREWFS_DATA_DIR=/var/lib/brewfs/data \
        -e BREWFS_CACHE_ROOT="/var/lib/brewfs/suite-cache/$workspace" \
        -e BREWFS_ARTIFACT_ROOT=/artifacts \
        -e BREWFS_FUSE_WRITEBACK="${BREWFS_FUSE_WRITEBACK:-1}" \
        -e PJDFSTEST_INCLUDE_PATTERNS="${PJDFSTEST_INCLUDE_PATTERNS:-}" \
        -e PJDFSTEST_SKIP_PATTERNS="${PJDFSTEST_SKIP_PATTERNS:-}" \
        -e PJDFSTEST_EXTRA_ARGS="${PJDFSTEST_EXTRA_ARGS:-}" \
        "$@" \
        "$image"
}

log "initializing workspace volume"
run_cli "${catalog_cli_args[@]}" init-volume --owner workspace-harness \
    >"$run_dir/init.json"
root_workspace="$(extract_workspace_id "$run_dir/init.json")"

log "seeding shared base through a real FUSE mount"
run_dual_harness seed "$root_workspace" "" "$run_dir/dual-mount"

log "sealing the base and creating $WORKSPACE_COUNT O(1) fork(s)"
run_cli "${catalog_cli_args[@]}" fork "$root_workspace" --count "$WORKSPACE_COUNT" \
    --owner workspace-harness >"$run_dir/fork.json"
mapfile -t fork_ids < <(extract_fork_ids "$run_dir/fork.json")
[[ ${#fork_ids[@]} -eq "$WORKSPACE_COUNT" ]] \
    || die "expected exactly $WORKSPACE_COUNT forked workspace(s)"
workspace_a="${fork_ids[0]}"
printf '%s\n' "$workspace_a" >"$run_dir/workspace-a.id"
if (( WORKSPACE_COUNT == 2 )); then
    workspace_b="${fork_ids[1]}"
    printf '%s\n' "$workspace_b" >"$run_dir/workspace-b.id"
fi

if (( WORKSPACE_COUNT == 2 )); then
    log "verifying simultaneous dual-workspace isolation"
    run_dual_harness verify "$workspace_a" "$workspace_b" "$run_dir/dual-mount"
else
    log "single-workspace mode: skipping dual-workspace isolation check"
fi

status=0
if [[ "$RUN_XFSTESTS" == true ]]; then
    if (( WORKSPACE_COUNT == 1 )); then
        log "running xfstests on workspace A"
        run_suite "$XFSTESTS_IMAGE" "$workspace_a" "$run_dir/xfstests-a" \
            -e XFSTESTS_CASES="$XFSTESTS_CASES" || status=1
    elif [[ "$PARALLEL_SUITES" == true ]]; then
        log "running xfstests concurrently on workspaces A and B"
        run_suite "$XFSTESTS_IMAGE" "$workspace_a" "$run_dir/xfstests-a" \
            -e XFSTESTS_CASES="$XFSTESTS_CASES" &
        pid_a=$!
        run_suite "$XFSTESTS_IMAGE" "$workspace_b" "$run_dir/xfstests-b" \
            -e XFSTESTS_CASES="$XFSTESTS_CASES" &
        pid_b=$!
        wait "$pid_a" || status=1
        wait "$pid_b" || status=1
    else
        log "running xfstests on workspace A"
        run_suite "$XFSTESTS_IMAGE" "$workspace_a" "$run_dir/xfstests-a" \
            -e XFSTESTS_CASES="$XFSTESTS_CASES" || status=1
        log "running xfstests on workspace B"
        run_suite "$XFSTESTS_IMAGE" "$workspace_b" "$run_dir/xfstests-b" \
            -e XFSTESTS_CASES="$XFSTESTS_CASES" || status=1
    fi
fi

if [[ "$RUN_PJDFSTEST" == true ]]; then
    if (( WORKSPACE_COUNT == 1 )); then
        log "running pjdfstest on workspace A"
        run_suite "$PJDFSTEST_IMAGE" "$workspace_a" "$run_dir/pjdfstest-a" || status=1
    elif [[ "$PARALLEL_SUITES" == true ]]; then
        log "running pjdfstest concurrently on workspaces A and B"
        run_suite "$PJDFSTEST_IMAGE" "$workspace_a" "$run_dir/pjdfstest-a" &
        pid_a=$!
        run_suite "$PJDFSTEST_IMAGE" "$workspace_b" "$run_dir/pjdfstest-b" &
        pid_b=$!
        wait "$pid_a" || status=1
        wait "$pid_b" || status=1
    else
        log "running pjdfstest on workspace A"
        run_suite "$PJDFSTEST_IMAGE" "$workspace_a" "$run_dir/pjdfstest-a" || status=1
        log "running pjdfstest on workspace B"
        run_suite "$PJDFSTEST_IMAGE" "$workspace_b" "$run_dir/pjdfstest-b" || status=1
    fi
fi

log "artifacts: $run_dir"
exit "$status"
