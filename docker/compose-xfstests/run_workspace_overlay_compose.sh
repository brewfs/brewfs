#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.workspace-overlay.yml"
ARTIFACT_ROOT="${BREWFS_WORKSPACE_ARTIFACT_ROOT:-$SCRIPT_DIR/artifacts}"
BACKEND="${BREWFS_WORKSPACE_META_BACKEND:-redis}"
XFSTESTS_CASES_VALUE="${XFSTESTS_CASES:-}"
RUN_XFSTESTS=true
RUN_LTP=true
BUILD_IMAGES=true
KEEP=false

log() { printf '[workspace-overlay-compose] %s\n' "$*"; }
die() { log "ERROR: $*" >&2; exit 2; }

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Run the workspace overlay integration stack with Docker Compose.  A shared
base is seeded through FUSE, forked into two workspaces, checked for isolation,
then each selected filesystem suite runs against both forks.

Options:
  --backend BACKEND          metadata backend: redis or tikv (default: $BACKEND)
  --xfstests-cases "CASES"   xfstests cases; omit for the complete configured suite
  --skip-xfstests            do not run xfstests
  --skip-ltp                 do not run LTP
  --ltp-skip-tests "NAMES"   extra LTP test names to skip
  --ltp-extra-args "ARGS"    extra arguments passed to runltp
  --reuse-images             reuse existing images and target/docker/brewfs
  --keep                     keep Compose services and volumes after the run
  -h, --help                 show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --backend)
            [[ $# -ge 2 ]] || die "--backend requires a value"
            BACKEND="$2"
            shift 2
            ;;
        --xfstests-cases)
            [[ $# -ge 2 ]] || die "--xfstests-cases requires a value"
            XFSTESTS_CASES_VALUE="$2"
            shift 2
            ;;
        --skip-xfstests)
            RUN_XFSTESTS=false
            shift
            ;;
        --skip-ltp)
            RUN_LTP=false
            shift
            ;;
        --ltp-skip-tests)
            [[ $# -ge 2 ]] || die "--ltp-skip-tests requires a value"
            export LTP_SKIP_TESTS="$2"
            shift 2
            ;;
        --ltp-extra-args)
            [[ $# -ge 2 ]] || die "--ltp-extra-args requires a value"
            export LTP_EXTRA_ARGS="$2"
            shift 2
            ;;
        --reuse-images)
            BUILD_IMAGES=false
            shift
            ;;
        --keep)
            KEEP=true
            shift
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

case "$BACKEND" in
    redis|tikv) ;;
    *) die "--backend must be redis or tikv" ;;
esac
[[ "$RUN_XFSTESTS" == true || "$RUN_LTP" == true ]] \
    || die "at least one of xfstests or LTP must be selected"

command -v cargo >/dev/null || die "cargo is required"
command -v docker >/dev/null || die "docker is required"
command -v git >/dev/null || die "git is required"
[[ -e /dev/fuse ]] || die "/dev/fuse is required"

mkdir -p "$ARTIFACT_ROOT"
export BREWFS_WORKSPACE_ARTIFACT_DIR="$ARTIFACT_ROOT"
export BREWFS_WORKSPACE_META_BACKEND="$BACKEND"
export BREWFS_WORKSPACE_META_URL="${BREWFS_WORKSPACE_META_URL:-redis://redis:6379/0}"
export BREWFS_WORKSPACE_TIKV_PD_ENDPOINTS="${BREWFS_WORKSPACE_TIKV_PD_ENDPOINTS:-pd:2379}"
export BREWFS_WORKSPACE_NAMESPACE="${BREWFS_WORKSPACE_NAMESPACE:-workspace-overlay-$(date +%s)-$RANDOM}"
export XFSTESTS_CASES="$XFSTESTS_CASES_VALUE"

ts="$(date +%Y%m%d-%H%M%S)-$RANDOM"
project_name="brewfs-workspace-overlay-$ts"
compose_args=(-f "$COMPOSE_FILE" -p "$project_name")

cleanup() {
    local status=$?
    set +e
    if [[ "$KEEP" == false ]]; then
        docker compose "${compose_args[@]}" --profile redis --profile tikv \
            down -v --remove-orphans >/dev/null 2>&1 || true
    else
        log "keeping Compose project $project_name (--keep)"
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

if [[ "$BUILD_IMAGES" == true ]]; then
    log "building workspace-overlay release binary"
    (
        cd "$PROJECT_DIR"
        CARGO_PROFILE_RELEASE_DEBUG=0 cargo build --release -p brewfs --bin brewfs \
            --features workspace-overlay
        mkdir -p target/docker
        install -m 755 target/release/brewfs target/docker/brewfs
    )

    log "building xfstests base image"
    docker compose "${compose_args[@]}" build --pull=false xfstests-base
    log "building workspace xfstests image"
    docker compose "${compose_args[@]}" build --pull=false xfstests
    if [[ "$RUN_LTP" == true ]]; then
        log "building LTP image"
        docker compose "${compose_args[@]}" build --pull=false ltp
    fi
fi

case "$BACKEND" in
    redis)
        log "starting Redis metadata service"
        docker compose "${compose_args[@]}" --profile redis up -d --wait redis
        ;;
    tikv)
        log "starting TiKV/PD metadata services"
        docker compose "${compose_args[@]}" --profile tikv up -d pd tikv
        docker compose "${compose_args[@]}" --profile tikv run --rm --no-deps tikv-ready
        ;;
esac

run_control() {
    local mode="$1"
    docker compose "${compose_args[@]}" run --rm --no-deps workspace-control "$mode"
}

log "initializing workspace catalog"
run_control init
log "seeding shared base through a FUSE mount"
run_control seed
log "forking two independent workspaces"
run_control fork
log "verifying simultaneous workspace isolation"
run_control verify

status=0
if [[ "$RUN_XFSTESTS" == true ]]; then
    log "running xfstests on both workspaces"
    set +e
    docker compose "${compose_args[@]}" run --rm --no-deps xfstests
    xfstests_status=$?
    set -e
    (( xfstests_status == 0 )) || status=1
fi

if [[ "$RUN_LTP" == true ]]; then
    log "running LTP filesystem tests on both workspaces"
    set +e
    docker compose "${compose_args[@]}" run --rm --no-deps ltp
    ltp_status=$?
    set -e
    (( ltp_status == 0 )) || status=1
fi

log "artifacts: $ARTIFACT_ROOT"
exit "$status"
