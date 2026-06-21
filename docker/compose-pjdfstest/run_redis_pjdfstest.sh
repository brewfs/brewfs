#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(realpath "$SCRIPT_DIR/..")"

COMPOSE_FILE="$SCRIPT_DIR/docker-compose.redis.yml"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Run pjdfstest against BrewFS in Docker Compose.

Options:
  --tests "<test...>"        Run selected pjdfstest tests, for example: "chmod chown"
  --prove-args "<args...>"   Pass extra arguments to prove, for example: "-v"
  --keep                     Do not run compose down after the test
  -h, --help                 Show this help

Artifacts:
  $ARTIFACTS_DIR/run-*/
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
PJDFSTEST_TESTS_VALUE=""
PJDFSTEST_PROVE_ARGS_VALUE=""

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --tests)
            require_value "$1" "${2:-}"
            PJDFSTEST_TESTS_VALUE="${2:-}"
            shift 2
            ;;
        --prove-args)
            require_value "$1" "${2:-}"
            PJDFSTEST_PROVE_ARGS_VALUE="${2:-}"
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
            err "unknown argument: $1"
            usage
            ;;
    esac
done

mkdir -p "$ARTIFACTS_DIR"

ts="$(date +%s)-$RANDOM"
PROJECT_NAME="brewfs-pjdfstest-${ts}"
COMPOSE_ARGS=(-f "$COMPOSE_FILE" -p "$PROJECT_NAME")

cleanup() {
    if [[ "$KEEP" == true ]]; then
        info "skip compose down (--keep)"
        return 0
    fi
    docker compose "${COMPOSE_ARGS[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

info "build host brewfs release binary for Docker COPY"
bash "$DOCKER_DIR/build_brewfs_host_binary.sh"

info "build pjdfstest runner image"
docker compose "${COMPOSE_ARGS[@]}" build pjdfstest

export BREWFS_ARTIFACT_DIR="/artifacts/run-${ts}"
export BREWFS_DATA_BACKEND="s3"
export PJDFSTEST_TESTS="$PJDFSTEST_TESTS_VALUE"
export PJDFSTEST_PROVE_ARGS="$PJDFSTEST_PROVE_ARGS_VALUE"

info "start dependency services: redis + rustfs"
docker compose "${COMPOSE_ARGS[@]}" up -d redis rustfs

info "initialize rustfs bucket"
docker compose "${COMPOSE_ARGS[@]}" run --rm rustfs-init

info "run pjdfstest"
set +e
docker compose "${COMPOSE_ARGS[@]}" run --rm pjdfstest
status=$?
set -e

ok "compose finished (exit=$status)"
ok "artifacts: $ARTIFACTS_DIR/run-${ts}"
exit "$status"
