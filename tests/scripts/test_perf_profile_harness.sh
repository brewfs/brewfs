#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

assert_file_contains() {
    local file="$1"
    local needle="$2"
    grep -qF "$needle" "$file" || fail "$file missing: $needle"
}

assert_file_not_contains() {
    local file="$1"
    local needle="$2"
    ! grep -qF "$needle" "$file" || fail "$file should not contain: $needle"
}

assert_manifest_keys() {
    local file="$1"
    shift

    assert_file_contains "$file" "perf-profile.env"
    assert_file_contains "$file" "write_perf_profile"
    assert_file_contains "$file" "write_perf_profile"

    for key in "$@"; do
        assert_file_contains "$file" "$key="
    done
}

redis_runner="$ROOT_DIR/docker/compose-xfstests/run_redis_perf.sh"
brewfs_container="$ROOT_DIR/docker/compose-xfstests/run_perf_in_container.sh"
juicefs_container="$ROOT_DIR/docker/compose-xfstests/run_juicefs_perf_in_container.sh"
juicefs_runner="$ROOT_DIR/docker/compose-xfstests/run_juicefs_perf.sh"
brewfs_compose="$ROOT_DIR/docker/compose-xfstests/docker-compose.redis-perf.yml"
juicefs_compose="$ROOT_DIR/docker/compose-xfstests/docker-compose.juicefs-perf.yml"

assert_runner_console_capture() {
    local file="$1"
    assert_file_contains "$file" "runner-console.log"
    assert_file_contains "$file" "runner-warning-summary.tsv"
    assert_file_contains "$file" "write_warning_summary"
    assert_file_contains "$file" 'tee "$runner_console_log"'
    assert_file_contains "$file" 'PIPESTATUS[0]'
}

assert_file_contains "$redis_runner" "compression=none"
assert_file_contains "$redis_runner" 'BREWFS_COMPRESSION="${BREWFS_COMPRESSION:-none}"'
assert_file_not_contains "$redis_runner" "compression=lz4"
assert_file_not_contains "$redis_runner" 'BREWFS_COMPRESSION="${BREWFS_COMPRESSION:-lz4}"'

common_keys=(
    PERF_TOOLS
    PERF_FIO_DIRECT
    PERF_FIO_DIRECT_MATRIX
    PERF_FIO_IOENGINE
    PERF_FIO_IODEPTH
    PERF_FIO_PREFILL_DRAIN
    PERF_FIO_PREFILL_REMOUNT
    PERF_FIO_COLD_READ_CLEAR_CACHE
    PERF_FIO_DROP_CACHES
)

assert_manifest_keys "$brewfs_container" \
    "${common_keys[@]}" \
    BREWFS_COMPRESSION \
    BREWFS_FUSE_WORKERS \
    BREWFS_FUSE_MAX_BACKGROUND \
    BREWFS_WRITEBACK_MODE \
    BREWFS_WRITEBACK_UPLOAD_CONCURRENCY \
    BREWFS_S3_MAX_CONCURRENCY \
    BREWFS_METADATA_OPEN_CACHE_TTL_MS \
    BREWFS_METADATA_OPEN_CACHE_CAPACITY \
    BREWFS_READ_SSD_BYTES \
    BREWFS_WRITE_SSD_BYTES \
    BREWFS_VERIFY_CACHE_CHECKSUM

assert_manifest_keys "$juicefs_container" \
    "${common_keys[@]}" \
    JFS_COMPRESS \
    JFS_WRITEBACK \
    JFS_MAX_UPLOADS \
    JFS_MAX_DOWNLOADS_EFFECTIVE \
    JFS_OPEN_CACHE \
    JFS_OPEN_CACHE_LIMIT

assert_runner_console_capture "$redis_runner"
assert_runner_console_capture "$juicefs_runner"

assert_file_contains "$redis_runner" "REDIS_PERF_DATA_MOUNT"
assert_file_contains "$juicefs_runner" "REDIS_PERF_DATA_MOUNT"
assert_file_contains "$brewfs_compose" '${REDIS_PERF_DATA_MOUNT:-redis-data-perf}'
assert_file_contains "$juicefs_compose" '${REDIS_PERF_DATA_MOUNT:-redis-data-juicefs-perf}'

assert_file_contains "$juicefs_runner" "PERF_FIO_DIRECT_MATRIX=\"0 1\""
assert_file_contains "$juicefs_runner" "PERF_FIO_SEQREAD_DIRECT_MATRIX"
assert_file_contains "$juicefs_runner" "PERF_FIO_BIGWRITE_DIRECT_MATRIX"
assert_file_contains "$juicefs_container" 'run_fio_profile "${tool}-direct${direct_value}"'

echo "perf profile harness checks passed"
