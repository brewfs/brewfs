#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(realpath "$SCRIPT_DIR/..")"

COMPOSE_FILE="$SCRIPT_DIR/docker-compose.pjdfstest-tikv.yml"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

usage() {
    cat <<EOF
usage: $(basename "$0") [options]

description:
  - run pjdfstest inside docker container against brewfs with TiKV metadata backend
  - object storage is rustfs (BREWFS_DATA_BACKEND=s3)
  - artifacts output to: $ARTIFACTS_DIR
  - set BREWFS_REUSE_HOST_BINARY=1 to reuse target/docker/brewfs during local reruns

options:
  --skip-patterns "<regex...>"   extra relative test-path regexes to skip
  --extra-args "<args...>"       extra arguments passed to prove
  --namespace <NAME>             TiKV metadata key namespace, default: brewfs
  --keep                         do not run compose down after exit (for debugging)
  -h, --help                     show help
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
PJDFSTEST_SKIP_PATTERNS_VALUE=""
PJDFSTEST_EXTRA_ARGS_VALUE=""
TIKV_NAMESPACE_VALUE="${BREWFS_META_TIKV_NAMESPACE:-brewfs}"

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --skip-patterns)
            require_value "$1" "${2:-}"
            PJDFSTEST_SKIP_PATTERNS_VALUE="${2:-}"
            shift 2
            ;;
        --extra-args)
            require_value "$1" "${2:-}"
            PJDFSTEST_EXTRA_ARGS_VALUE="${2:-}"
            shift 2
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

ts="$(date +%s)-$RANDOM"
PROJECT_NAME="brewfs-pjdfstest-tikv-${ts}"
COMPOSE_ARGS=(-f "$COMPOSE_FILE" -p "$PROJECT_NAME")
DEFAULT_RUSTFS_DATA_HOST_DIR="$ARTIFACTS_DIR/rustfs-data-pjdfstest-tikv-${ts}"
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

info "build pjdfstest runner image"
docker compose "${COMPOSE_ARGS[@]}" build pjdfstest

export BREWFS_ARTIFACT_DIR="/artifacts/pjdfstest-run-${ts}"
export BREWFS_META_TIKV_NAMESPACE="$TIKV_NAMESPACE_VALUE"
export PJDFSTEST_SKIP_PATTERNS="${PJDFSTEST_SKIP_PATTERNS_VALUE:-}"
export PJDFSTEST_EXTRA_ARGS="${PJDFSTEST_EXTRA_ARGS_VALUE:-}"

info "start dependency services: pd + tikv + tikv-ready + rustfs"
docker compose "${COMPOSE_ARGS[@]}" up -d pd tikv tikv-ready rustfs

info "initialize rustfs bucket (one-shot container)"
docker compose "${COMPOSE_ARGS[@]}" run --rm rustfs-init

info "run pjdfstest (exit code from pjdfstest container)"
set +e
docker compose "${COMPOSE_ARGS[@]}" run --rm pjdfstest
status=$?
set -e

ok "compose run finished (exit=$status)"
ok "artifact dir: $ARTIFACTS_DIR/pjdfstest-run-${ts}"
exit "$status"
