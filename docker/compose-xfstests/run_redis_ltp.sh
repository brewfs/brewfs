#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(realpath "$SCRIPT_DIR/..")"
PROJECT_DIR="$(realpath "$DOCKER_DIR/../..")"

COMPOSE_FILE="$SCRIPT_DIR/docker-compose.ltp-redis.yml"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

usage() {
    cat <<EOF
usage: $(basename "$0") [options]

description:
  - run LTP filesystem tests inside docker container against brewfs with redis metadata backend
  - object storage is rustfs (BREWFS_DATA_BACKEND=s3)
  - artifacts output to: $ARTIFACTS_DIR

options:
  --skip-tests "<case...>"      extra testcase names to skip
  --extra-args "<args...>"      extra arguments passed to runltp
  --no-default-skip             ignore the built-in BrewFS LTP skip file
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
        --keep)
            KEEP=true
            shift
            ;;
        --no-default-skip)
            NO_DEFAULT_SKIP=true
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

ts="$(date +%s)-$RANDOM"
PROJECT_NAME="brewfs-ltp-redis-${ts}"
COMPOSE_ARGS=(-f "$COMPOSE_FILE" -p "$PROJECT_NAME")

cleanup() {
    if [[ "$KEEP" == true ]]; then
        info "skip compose down (--keep)"
        return 0
    fi
    docker compose "${COMPOSE_ARGS[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

info "build brewfs release binary on host (for COPY in Dockerfile)"
bash "$DOCKER_DIR/build_brewfs_host_binary.sh"

info "build LTP runner image"
docker compose "${COMPOSE_ARGS[@]}" build ltp
export BREWFS_ARTIFACT_DIR="/artifacts/run-${ts}"
export LTP_SKIP_TESTS="${LTP_SKIP_TESTS_VALUE:-}"
export LTP_EXTRA_ARGS="${LTP_EXTRA_ARGS_VALUE:-}"
if [[ "$NO_DEFAULT_SKIP" == true ]]; then
    export LTP_DEFAULT_SKIP_TESTS_FILE="/dev/null"
fi

info "start dependency services: redis + rustfs"
docker compose "${COMPOSE_ARGS[@]}" up -d redis rustfs

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
