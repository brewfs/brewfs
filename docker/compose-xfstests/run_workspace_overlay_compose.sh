#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.workspace.yml"
ARTIFACT_ROOT="${BREWFS_WORKSPACE_ARTIFACT_ROOT:-$SCRIPT_DIR/artifacts}"
BACKEND="${BREWFS_WORKSPACE_META_BACKEND:-redis}"
XFSTESTS_CASES_VALUE="${XFSTESTS_CASES:-}"
RUN_XFSTESTS=true
RUN_LTP=true
CONTROL_ONLY=false
BUILD_IMAGES=true
KEEP=false

log() { printf '[workspace-compose] %s\n' "$*"; }
die() { log "ERROR: $*" >&2; exit 2; }

usage() {
    cat <<EOF
Usage: $(basename "$0") [options]

Run the workspace overlay integration stack with Docker Compose.  A shared
base is packed into an immutable lower, forked into two workspaces, checked for
isolation, then concurrent fork checks and selected filesystem suites run.

Options:
  --backend BACKEND          metadata backend: redis or tikv (default: $BACKEND)
  --xfstests-cases "CASES"   xfstests cases; omit for the complete configured suite
  --skip-xfstests            do not run xfstests
  --skip-ltp                 do not run LTP
  --control-only             run init/fork/isolation/concurrency verification
  --ltp-skip-tests "NAMES"   extra LTP test names to skip
  --ltp-extra-args "ARGS"    extra arguments passed to runltp
  --reuse-images             reuse existing images (does not rebuild current HEAD)
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
        --control-only)
            RUN_XFSTESTS=false
            RUN_LTP=false
            CONTROL_ONLY=true
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
[[ "$CONTROL_ONLY" == true || "$RUN_XFSTESTS" == true || "$RUN_LTP" == true ]] \
    || die "at least one of xfstests or LTP must be selected"

command -v cargo >/dev/null || die "cargo is required"
command -v docker >/dev/null || die "docker is required"
command -v git >/dev/null || die "git is required"
command -v python3 >/dev/null || die "python3 is required"
[[ -e /dev/fuse ]] || die "/dev/fuse is required"

mkdir -p "$ARTIFACT_ROOT"
export BREWFS_WORKSPACE_META_BACKEND="$BACKEND"
export BREWFS_WORKSPACE_META_URL="${BREWFS_WORKSPACE_META_URL:-redis://redis:6379/0}"
export BREWFS_WORKSPACE_TIKV_PD_ENDPOINTS="${BREWFS_WORKSPACE_TIKV_PD_ENDPOINTS:-pd:2379}"
export XFSTESTS_CASES="$XFSTESTS_CASES_VALUE"

ts="$(date +%Y%m%d-%H%M%S)-$RANDOM"
project_name="brewfs-workspace-$ts"
run_dir="$ARTIFACT_ROOT/workspace-$ts"
mkdir -p "$run_dir"
export BREWFS_WORKSPACE_ARTIFACT_DIR="$run_dir"
export BREWFS_WORKSPACE_NAMESPACE="${BREWFS_WORKSPACE_NAMESPACE:-workspace-$ts}"
compose_args=(-f "$COMPOSE_FILE" -p "$project_name")
journal_helper="$PROJECT_DIR/tools/perf/workspace_control_resource_journal.py"
journal_path="$run_dir/workspace-control-resource-journal.json"
python3 "$journal_helper" init \
    --path "$journal_path" \
    --run-id "$ts" \
    --artifact "$run_dir" \
    --project "$project_name" \
    --backend "$BACKEND" \
    --namespace "$BREWFS_WORKSPACE_NAMESPACE"

journal_stage_start() {
    local stage="$1"
    python3 "$journal_helper" stage \
        --path "$journal_path" --stage "$stage" --phase start
    python3 "$journal_helper" snapshot \
        --path "$journal_path" --stage "$stage" --phase start \
        --project "$project_name"
}

journal_stage_end() {
    local stage="$1"
    local stage_status="$2"
    set +e
    python3 "$journal_helper" snapshot \
        --path "$journal_path" --stage "$stage" --phase end \
        --project "$project_name" || log "resource snapshot failed for stage $stage"
    python3 "$journal_helper" stage \
        --path "$journal_path" --stage "$stage" --phase end \
        --status "$stage_status" || log "resource journal stage update failed for $stage"
    set -e
}

run_stage() {
    local stage="$1"
    shift
    journal_stage_start "$stage"
    local stage_status
    set +e
    "$@"
    stage_status=$?
    journal_stage_end "$stage" "$stage_status"
    set -e
    return "$stage_status"
}

cleanup() {
    local status=$?
    local down_status=0
    local clean_status=0
    local snapshot_status=0
    local cleanup_stage_status=0
    local final_status="$status"
    set +e
    python3 "$journal_helper" stage \
        --path "$journal_path" --stage cleanup --phase start || true
    python3 "$journal_helper" snapshot \
        --path "$journal_path" --stage cleanup --phase start \
        --project "$project_name" || snapshot_status=1
    if [[ "$KEEP" == false ]]; then
        log "stopping Compose project $project_name"
        docker compose "${compose_args[@]}" --profile redis --profile tikv \
            down -v --remove-orphans >"$run_dir/compose-down.log" 2>&1
        down_status=$?
        if (( down_status != 0 )); then
            log "Compose cleanup failed with status $down_status; see $run_dir/compose-down.log"
        fi
    else
        log "keeping Compose project $project_name (--keep)"
    fi
    python3 "$journal_helper" snapshot \
        --path "$journal_path" --stage cleanup --phase cleanup \
        --project "$project_name" || snapshot_status=1
    if [[ "$KEEP" == false ]]; then
        python3 "$journal_helper" check-empty --path "$journal_path"
        clean_status=$?
        if (( clean_status != 0 )); then
            log "Compose project resources remain after cleanup; inspect $journal_path"
        fi
    fi
    if (( snapshot_status != 0 )); then
        clean_status=1
        log "resource cleanup snapshot failed; inspect $journal_path"
    fi
    if (( down_status != 0 && final_status == 0 )); then
        final_status="$down_status"
    elif (( clean_status != 0 && final_status == 0 )); then
        final_status=1
    fi
    if (( down_status != 0 || clean_status != 0 )); then
        cleanup_stage_status=1
    fi
    python3 "$journal_helper" stage \
        --path "$journal_path" --stage cleanup --phase end \
        --status "$cleanup_stage_status" || true
    finalize_args=(
        python3 "$journal_helper" finalize
        --path "$journal_path"
        --status "$final_status"
        --down-status "$down_status"
    )
    if [[ "$KEEP" == true ]]; then
        finalize_args+=(--kept)
    fi
    if ! "${finalize_args[@]}"; then
        log "resource journal finalization failed"
        final_status=1
    fi
    set -e
    exit "$final_status"
}
trap cleanup EXIT INT TERM

build_release() {
    (
        cd "$PROJECT_DIR" || exit
        CARGO_PROFILE_RELEASE_DEBUG=0 cargo build --release -p brewfs \
            --bin brewfs --bin packed_snapshot_fixture --features workspace || exit
        mkdir -p target/docker || exit
        install -m 755 target/release/brewfs target/release/packed_snapshot_fixture target/docker/
    )
}

build_images() {
    log "building xfstests base image"
    docker compose "${compose_args[@]}" build --pull=false xfstests-base || return
    log "building workspace xfstests image"
    docker compose "${compose_args[@]}" build --pull=false xfstests || return
    if [[ "$RUN_LTP" == true ]]; then
        log "building LTP image"
        docker compose "${compose_args[@]}" build --pull=false ltp
    fi
}

if [[ "$BUILD_IMAGES" == true ]]; then
    log "building workspace release binary"
    run_stage build-release build_release
    run_stage build-images build_images
fi

start_backend() {
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
}

run_stage backend-start start_backend

run_control() {
    local mode="$1"
    docker compose "${compose_args[@]}" run --rm --no-deps workspace-control "$mode"
}

log "initializing workspace catalog"
run_stage init run_control init
log "forking two independent workspaces"
run_stage fork run_control fork
log "verifying simultaneous workspace isolation"
run_stage verify run_control verify
log "verifying concurrent forks with two mounted writers"
run_stage concurrent-fork run_control concurrent-fork

status=0
if [[ "$RUN_XFSTESTS" == true ]]; then
    log "running xfstests on both workspaces"
    if run_stage xfstests docker compose "${compose_args[@]}" run --rm --no-deps xfstests; then
        xfstests_status=0
    else
        xfstests_status=$?
    fi
    (( xfstests_status == 0 )) || status=1
fi

if [[ "$RUN_LTP" == true ]]; then
    log "running LTP filesystem tests on both workspaces"
    if run_stage ltp docker compose "${compose_args[@]}" run --rm --no-deps ltp; then
        ltp_status=0
    else
        ltp_status=$?
    fi
    (( ltp_status == 0 )) || status=1
fi

log "artifacts: $run_dir"
exit "$status"
