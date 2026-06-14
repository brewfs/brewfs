#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_PERF="$SCRIPT_DIR/run_redis_perf.sh"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
err()  { log "ERROR $*" >&2; }

usage() {
    cat <<EOF
usage: $(basename "$0") [options] [-- extra run_redis_perf.sh args]

description:
  - run BrewFS stress-ng profiles through the existing Redis perf compose runner
  - default storage backend is rustfs S3, matching the normal perf path
  - artifacts output to: $SCRIPT_DIR/artifacts/perf-run-*

options:
  --profile <name>   profile to run: smoke, metadata-heavy, link-symlink-heavy, write-smallfile
  --s3               use rustfs object storage (default)
  --minio            use MinIO object storage
  --local-fs         use local object directory
  --keep             pass through to run_redis_perf.sh
  -h, --help         show help

profiles:
  smoke                short CI-safe dir/dentry/rename/unlink/hdd mix
  metadata-heavy       higher metadata op counts, no link/symlink long-tail stressors
  link-symlink-heavy   explicit hardlink/symlink stress; useful for manual regression hunting
  write-smallfile      small-block hdd/unlink pressure on the mounted filesystem

environment:
  PERF_STRESS_NG_ARGS fully overrides profile-generated stress-ng arguments.
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

export_default() {
    local name="$1"
    local value="$2"
    if [[ -z "${!name:-}" ]]; then
        export "$name=$value"
    fi
}

profile="smoke"
storage_arg="--s3"
extra_args=()

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --profile)
            require_value "$1" "${2:-}"
            profile="${2:-}"
            shift 2
            ;;
        --s3|--minio|--local-fs)
            storage_arg="$1"
            shift
            ;;
        --keep|--s3-writeback|--writeback-throughput-profile)
            extra_args+=("$1")
            shift
            ;;
        --)
            shift
            extra_args+=("$@")
            break
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

case "$profile" in
    smoke)
        export_default PERF_STRESS_NG_TIMEOUT 10s
        export_default PERF_STRESS_NG_DIR_WORKERS 1
        export_default PERF_STRESS_NG_DIR_OPS 1000
        export_default PERF_STRESS_NG_DENTRY_WORKERS 1
        export_default PERF_STRESS_NG_DENTRY_OPS 100
        export_default PERF_STRESS_NG_RENAME_WORKERS 1
        export_default PERF_STRESS_NG_RENAME_OPS 1000
        export_default PERF_STRESS_NG_UNLINK_WORKERS 1
        export_default PERF_STRESS_NG_UNLINK_OPS 500
        export_default PERF_STRESS_NG_HDD_WORKERS 1
        export_default PERF_STRESS_NG_HDD_BYTES 8M
        export_default PERF_STRESS_NG_HDD_WRITE_SIZE 128K
        ;;
    metadata-heavy)
        export_default PERF_STRESS_NG_TIMEOUT 60s
        export_default PERF_STRESS_NG_DIR_WORKERS 2
        export_default PERF_STRESS_NG_DIR_OPS 5000
        export_default PERF_STRESS_NG_DENTRY_WORKERS 2
        export_default PERF_STRESS_NG_DENTRY_OPS 1000
        export_default PERF_STRESS_NG_RENAME_WORKERS 2
        export_default PERF_STRESS_NG_RENAME_OPS 5000
        export_default PERF_STRESS_NG_UNLINK_WORKERS 2
        export_default PERF_STRESS_NG_UNLINK_OPS 2000
        export_default PERF_STRESS_NG_HDD_WORKERS 1
        export_default PERF_STRESS_NG_HDD_BYTES 16M
        export_default PERF_STRESS_NG_HDD_WRITE_SIZE 64K
        ;;
    link-symlink-heavy)
        if [[ -z "${PERF_STRESS_NG_ARGS:-}" ]]; then
            export PERF_STRESS_NG_ARGS="--temp-path /mnt/brewfs/.perf-stress-ng --timeout ${PERF_STRESS_NG_TIMEOUT:-30s} --metrics-brief --verify --link ${PERF_STRESS_NG_LINK_WORKERS:-1} --link-ops ${PERF_STRESS_NG_LINK_OPS:-20} --symlink ${PERF_STRESS_NG_SYMLINK_WORKERS:-1} --symlink-ops ${PERF_STRESS_NG_SYMLINK_OPS:-20} --rename 1 --rename-ops 200 --dir 1 --dir-ops 200"
        fi
        ;;
    write-smallfile)
        if [[ -z "${PERF_STRESS_NG_ARGS:-}" ]]; then
            export PERF_STRESS_NG_ARGS="--temp-path /mnt/brewfs/.perf-stress-ng --timeout ${PERF_STRESS_NG_TIMEOUT:-45s} --metrics-brief --verify --hdd ${PERF_STRESS_NG_HDD_WORKERS:-2} --hdd-bytes ${PERF_STRESS_NG_HDD_BYTES:-64M} --hdd-write-size ${PERF_STRESS_NG_HDD_WRITE_SIZE:-4K} --unlink 1 --unlink-ops ${PERF_STRESS_NG_UNLINK_OPS:-1000}"
        fi
        ;;
    *)
        err "unknown profile: $profile"
        usage
        ;;
esac

info "stress-ng profile: $profile"
export BREWFS_PERF_DOCKERFILE="${BREWFS_PERF_DOCKERFILE:-docker/compose-xfstests/Dockerfile.stress-ng}"
exec "$RUN_PERF" "$storage_arg" --tools stress-ng "${extra_args[@]}"
