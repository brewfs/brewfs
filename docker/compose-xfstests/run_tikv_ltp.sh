#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(realpath "$SCRIPT_DIR/..")"

COMPOSE_FILE="$SCRIPT_DIR/docker-compose.ltp-tikv.yml"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

usage() {
    cat <<EOF
usage: $(basename "$0") [options]

description:
  - run LTP filesystem tests inside docker container against brewfs with TiKV metadata backend
  - object storage is rustfs (BREWFS_DATA_BACKEND=s3)
  - artifacts output to: $ARTIFACTS_DIR
  - set BREWFS_REUSE_HOST_BINARY=1 to reuse target/docker/brewfs during local reruns

options:
  --skip-tests "<case...>"      extra testcase names to skip
  --extra-args "<args...>"      extra arguments passed to runltp
  --no-default-skip             ignore the built-in BrewFS LTP skip file
  --namespace <NAME>            TiKV metadata key namespace, default: brewfs
  --keep                        do not run compose down after exit (for debugging)
  -h, --help                    show help
EOF
    exit 0
}

require_value() {
    local option="$1"
    local value="${2:-}"
    if [[ -z "$value" ]]; then
        err "$option requires a value"
        exit 1
    fi
}

KEEP=false
NO_DEFAULT_SKIP=false
LTP_SKIP_TESTS_VALUE=""
LTP_EXTRA_ARGS_VALUE=""
TIKV_NAMESPACE_VALUE="${BREWFS_META_TIKV_NAMESPACE:-brewfs}"

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --skip-tests)
            require_value "$1" "${2:-}"
            LTP_SKIP_TESTS_VALUE="${2:-}"
            shift 2
            ;;
        --extra-args)
            require_value "$1" "${2:-}"
            LTP_EXTRA_ARGS_VALUE="${2:-}"
            shift 2
            ;;
        --no-default-skip)
            NO_DEFAULT_SKIP=true
            shift
            ;;
        --namespace)
            require_value "$1" "${2:-}"
            TIKV_NAMESPACE_VALUE="${2:-}"
            shift 2
            ;;
        --keep)
            KEEP=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            err "unknown arg: $1"
            usage
            ;;
    esac
done

mkdir -p "$ARTIFACTS_DIR"

# LTP rwtest/growfiles are correctness stressors, not throughput benchmarks.
# Use the same bounded cache profile as xfstests so random buffered writes drain
# steadily inside Docker instead of building a large local writeback backlog.
export BREWFS_CACHE_ROOT="${BREWFS_CACHE_ROOT:-/var/lib/brewfs/cache}"
export BREWFS_READ_MEMORY_BYTES="${BREWFS_READ_MEMORY_BYTES:-268435456}"
export BREWFS_READ_SSD_BYTES="${BREWFS_READ_SSD_BYTES:-1048576}"
export BREWFS_WRITE_MEMORY_BYTES="${BREWFS_WRITE_MEMORY_BYTES:-134217728}"
export BREWFS_WRITE_SSD_BYTES="${BREWFS_WRITE_SSD_BYTES:-1048576}"
export BREWFS_DIRTY_SLICE_TARGET_SIZE="${BREWFS_DIRTY_SLICE_TARGET_SIZE:-4194304}"
export BREWFS_DIRTY_SLICE_MAX_AGE_MS="${BREWFS_DIRTY_SLICE_MAX_AGE_MS:-500}"
export BREWFS_UPLOAD_CONCURRENCY="${BREWFS_UPLOAD_CONCURRENCY:-4}"
export BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD="${BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD:-false}"
export BREWFS_MEMORY_BUDGET_BYTES="${BREWFS_MEMORY_BUDGET_BYTES:-536870912}"

ts="$(date +%s)-$RANDOM"
PROJECT_NAME="brewfs-ltp-tikv-${ts}"
COMPOSE_ARGS=(-f "$COMPOSE_FILE" -p "$PROJECT_NAME")
DEFAULT_RUSTFS_DATA_HOST_DIR="$ARTIFACTS_DIR/rustfs-data-ltp-tikv-${ts}"
RUSTFS_DATA_HOST_DIR_WAS_SET="${RUSTFS_DATA_HOST_DIR:+1}"
export RUSTFS_DATA_HOST_DIR="${RUSTFS_DATA_HOST_DIR:-$DEFAULT_RUSTFS_DATA_HOST_DIR}"
mkdir -p "$RUSTFS_DATA_HOST_DIR"
chmod 0777 "$RUSTFS_DATA_HOST_DIR"

cleanup() {
    if [[ "$KEEP" == true ]]; then
        info "skip compose down (--keep)"
        return 0
    fi
    docker compose "${COMPOSE_ARGS[@]}" down -v >/dev/null 2>&1 || true
    if [[ -z "$RUSTFS_DATA_HOST_DIR_WAS_SET" ]]; then
        rm -rf "$RUSTFS_DATA_HOST_DIR"
    fi
}
trap cleanup EXIT INT TERM

info "build brewfs release binary on host (for COPY in Dockerfile)"
bash "$DOCKER_DIR/build_brewfs_host_binary.sh"

info "build LTP runner image"
docker compose "${COMPOSE_ARGS[@]}" build ltp
export BREWFS_ARTIFACT_DIR="/artifacts/run-${ts}"
export BREWFS_META_TIKV_NAMESPACE="$TIKV_NAMESPACE_VALUE"
export LTP_SKIP_TESTS="${LTP_SKIP_TESTS_VALUE:-}"
export LTP_EXTRA_ARGS="${LTP_EXTRA_ARGS_VALUE:-}"
if [[ "$NO_DEFAULT_SKIP" == true ]]; then
    export LTP_DEFAULT_SKIP_TESTS_FILE="/dev/null"
fi

info "start dependency services: pd + tikv + tikv-ready + rustfs"
docker compose "${COMPOSE_ARGS[@]}" up -d pd tikv tikv-ready rustfs

info "initialize rustfs bucket (one-shot container)"
docker compose "${COMPOSE_ARGS[@]}" run --rm rustfs-init

info "run LTP tests (exit code from ltp container)"
set +e
docker compose "${COMPOSE_ARGS[@]}" run --rm ltp
status=$?
set -e

ok "compose run finished (exit=$status)"
ok "artifact dir: $ARTIFACTS_DIR/run-${ts}"
exit "$status"
