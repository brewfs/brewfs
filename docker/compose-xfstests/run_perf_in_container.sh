#!/usr/bin/env bash

set -euo pipefail

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

config_path="${BREWFS_CONFIG_PATH:-/run/brewfs/config.yaml}"
mount_dir="${BREWFS_MOUNT_POINT:-/mnt/brewfs}"
data_backend="${BREWFS_DATA_BACKEND:-local-fs}"
data_dir="${BREWFS_DATA_DIR:-${BREWFS_HOME:-/var/lib/brewfs}/data}"
meta_backend="${BREWFS_META_BACKEND:-redis}"
meta_url="${BREWFS_META_URL:-}"
meta_etcd_urls="${BREWFS_META_ETCD_URLS:-http://etcd:2379}"
meta_tikv_pd_endpoints="${BREWFS_META_TIKV_PD_ENDPOINTS:-pd:2379}"
meta_tikv_namespace="${BREWFS_META_TIKV_NAMESPACE:-brewfs}"
sqlite_path="${BREWFS_SQLITE_PATH:-${BREWFS_HOME:-/var/lib/brewfs}/metadata.db}"
log_file="${BREWFS_LOG_FILE:-/artifacts/brewfs.log}"
xfstests_dir="${XFSTESTS_DIR:-/opt/xfstests-dev}"
artifact_root="${BREWFS_ARTIFACT_ROOT:-/artifacts}"
artifact_dir="${BREWFS_ARTIFACT_DIR:-}"
perf_tools="${PERF_TOOLS:-fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest}"
nofile_limit="${BREWFS_NOFILE_LIMIT:-1048576}"

env_or_default() {
    local specific_var="$1"
    local common_var="$2"
    local default_value="$3"
    local value="${!specific_var:-}"
    if [[ -n "$value" ]]; then
        printf '%s' "$value"
    else
        printf '%s' "${!common_var:-$default_value}"
    fi
}

truthy_env() {
    local value
    value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
    case "$value" in
        1|true|yes|on) return 0 ;;
        *) return 1 ;;
    esac
}

write_perf_profile() {
    local path="$artifact_dir/perf-profile.env"
    cat >"$path" <<EOF
PERF_TOOLS=${perf_tools}
PERF_FIO_DIRECT=${PERF_FIO_DIRECT:-0}
PERF_FIO_IOENGINE=${PERF_FIO_IOENGINE:-io_uring}
PERF_FIO_IODEPTH=${PERF_FIO_IODEPTH:-1}
PERF_FIO_PREFILL_DRAIN=${PERF_FIO_PREFILL_DRAIN:-false}
PERF_FIO_PREFILL_REMOUNT=${PERF_FIO_PREFILL_REMOUNT:-false}
PERF_FIO_COLD_READ_CLEAR_CACHE=${PERF_FIO_COLD_READ_CLEAR_CACHE:-false}
PERF_FIO_DROP_CACHES=${PERF_FIO_DROP_CACHES:-false}
PERF_FIO_COLD_READ=${PERF_FIO_COLD_READ:-false}
PERF_FIO_COLD_READ_DROP_CACHES=${PERF_FIO_COLD_READ_DROP_CACHES:-false}
PERF_FIO_POST_WRITE_DRAIN=${PERF_FIO_POST_WRITE_DRAIN:-false}
PERF_FIO_DIRECT_MATRIX=${PERF_FIO_DIRECT_MATRIX:-}
BREWFS_DATA_BACKEND=${data_backend}
BREWFS_META_BACKEND=${meta_backend}
BREWFS_CHUNK_SIZE=${BREWFS_CHUNK_SIZE:-67108864}
BREWFS_BLOCK_SIZE=${BREWFS_BLOCK_SIZE:-4194304}
BREWFS_COMPRESSION=${BREWFS_COMPRESSION:-none}
BREWFS_FUSE_WORKERS=${BREWFS_FUSE_WORKERS:-}
BREWFS_FUSE_MAX_BACKGROUND=${BREWFS_FUSE_MAX_BACKGROUND:-}
BREWFS_FUSE_WRITEBACK=${BREWFS_FUSE_WRITEBACK:-}
BREWFS_WRITEBACK_MODE=${BREWFS_WRITEBACK_MODE:-}
BREWFS_WRITEBACK_UPLOAD_CONCURRENCY=${BREWFS_WRITEBACK_UPLOAD_CONCURRENCY:-}
BREWFS_S3_MAX_CONCURRENCY=${BREWFS_S3_MAX_CONCURRENCY:-}
BREWFS_UPLOAD_CONCURRENCY=${BREWFS_UPLOAD_CONCURRENCY:-}
BREWFS_METADATA_OPEN_CACHE_TTL_MS=${BREWFS_METADATA_OPEN_CACHE_TTL_MS:-}
BREWFS_METADATA_OPEN_CACHE_CAPACITY=${BREWFS_METADATA_OPEN_CACHE_CAPACITY:-}
BREWFS_READ_MEMORY_BYTES=${BREWFS_READ_MEMORY_BYTES:-}
BREWFS_READ_SSD_BYTES=${BREWFS_READ_SSD_BYTES:-}
BREWFS_WRITE_MEMORY_BYTES=${BREWFS_WRITE_MEMORY_BYTES:-}
BREWFS_WRITE_SSD_BYTES=${BREWFS_WRITE_SSD_BYTES:-}
BREWFS_MEMORY_BUDGET_BYTES=${BREWFS_MEMORY_BUDGET_BYTES:-}
BREWFS_VERIFY_CACHE_CHECKSUM=${BREWFS_VERIFY_CACHE_CHECKSUM:-}
BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD=${BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD:-}
BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES=${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES:-}
BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES=${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES:-}
BREWFS_WRITEBACK_PERSIST_SYNC=${BREWFS_WRITEBACK_PERSIST_SYNC:-}
BREWFS_CACHED_SUB_BLOCK_IDLE_GRACE_MS=${BREWFS_CACHED_SUB_BLOCK_IDLE_GRACE_MS:-}
BREWFS_CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE_MS=${BREWFS_CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE_MS:-}
PERF_OBJECT_PUT_DURATION_SECS=${PERF_OBJECT_PUT_DURATION_SECS:-60}
PERF_OBJECT_PUT_OBJECTS=${PERF_OBJECT_PUT_OBJECTS:-0}
PERF_OBJECT_PUT_OBJECT_SIZE=${PERF_OBJECT_PUT_OBJECT_SIZE:-${BREWFS_BLOCK_SIZE:-4194304}}
PERF_OBJECT_PUT_WORKERS=${PERF_OBJECT_PUT_WORKERS:-${BREWFS_UPLOAD_CONCURRENCY:-32}}
PERF_OBJECT_PUT_PREFIX=${PERF_OBJECT_PUT_PREFIX:-bench/direct-put}
EOF

    {
        echo
        echo "# Raw PERF_FIO environment"
        env | sort | grep '^PERF_FIO_' || true
    } >>"$path"
}

raise_nofile_limit() {
    if ulimit -n "$nofile_limit" >/dev/null 2>&1; then
        info "nofile limit: $(ulimit -n)"
    else
        err "无法提升 nofile limit 到 $nofile_limit，当前: $(ulimit -n)"
    fi
}

write_config() {
    mkdir -p "$(dirname "$config_path")" "$mount_dir"
    if [[ "$data_backend" == "local-fs" ]]; then
        mkdir -p "$data_dir"
    fi

    {
        echo "mount_point: $mount_dir"
        echo
        case "$data_backend" in
            local-fs)
                cat <<EOF
data:
  backend: local-fs
  localfs:
    data_dir: ${data_dir}
EOF
                ;;
            s3)
                bucket="${BREWFS_S3_BUCKET:-brewfs-data}"
                region="${BREWFS_S3_REGION:-us-east-1}"
                endpoint="${BREWFS_S3_ENDPOINT:-http://rustfs:9000}"
                force_path="${BREWFS_S3_FORCE_PATH_STYLE:-true}"
                part_size="${BREWFS_S3_PART_SIZE:-16777216}"
                max_conc="${BREWFS_S3_MAX_CONCURRENCY:-8}"
                cat <<EOF
data:
  backend: s3
  s3:
    bucket: ${bucket}
    region: ${region}
    part_size: ${part_size}
    max_concurrency: ${max_conc}
    force_path_style: ${force_path}
    endpoint: ${endpoint}
EOF
                ;;
            *)
                err "不支持的 BREWFS_DATA_BACKEND: $data_backend"
                exit 1
                ;;
        esac
        echo

        case "$meta_backend" in
            sqlite)
                mkdir -p "$(dirname "$sqlite_path")"
                local url="${meta_url:-sqlite://${sqlite_path}?mode=rwc}"
                cat <<EOF
meta:
  backend: sqlx
  sqlx:
    url: "$url"
EOF
                ;;
            redis)
                if [[ -z "$meta_url" ]]; then
                    err "BREWFS_META_URL 不能为空 (redis)"
                    exit 1
                fi
                cat <<EOF
meta:
  backend: redis
  redis:
    url: "$meta_url"
EOF
                ;;
            etcd)
                cat <<EOF
meta:
  backend: etcd
  etcd:
    urls:
EOF
                local old_ifs="$IFS"
                IFS=','
                for url in $meta_etcd_urls; do
                    echo "      - \"${url}\""
                done
                IFS="$old_ifs"
                ;;
            tikv)
                cat <<EOF
meta:
  backend: tikv
  tikv:
    pd_endpoints:
EOF
                local old_ifs="$IFS"
                IFS=','
                for endpoint in $meta_tikv_pd_endpoints; do
                    echo "      - \"${endpoint}\""
                done
                IFS="$old_ifs"
                echo "    namespace: \"${meta_tikv_namespace}\""
                ;;
            *)
                err "不支持的 BREWFS_META_BACKEND: $meta_backend"
                exit 1
                ;;
        esac
        if [[ -n "${BREWFS_METADATA_OPEN_CACHE_TTL_MS:-}" ]]; then
            echo "  open_file_cache_ttl_ms: ${BREWFS_METADATA_OPEN_CACHE_TTL_MS}"
        fi
        if [[ -n "${BREWFS_METADATA_OPEN_CACHE_CAPACITY:-}" ]]; then
            echo "  open_file_cache_capacity: ${BREWFS_METADATA_OPEN_CACHE_CAPACITY}"
        fi

        if [[ -n "${BREWFS_COMPACT_INTERVAL_SECS:-}" \
            || -n "${BREWFS_COMPACT_MIN_SLICE_COUNT:-}" \
            || -n "${BREWFS_COMPACT_MIN_FRAGMENT_RATIO:-}" \
            || -n "${BREWFS_COMPACT_ASYNC_THRESHOLD:-}" \
            || -n "${BREWFS_COMPACT_SYNC_THRESHOLD:-}" \
            || -n "${BREWFS_COMPACT_MAX_CHUNKS_PER_RUN:-}" \
            || -n "${BREWFS_COMPACT_MAX_CONCURRENT_TASKS:-}" \
            || -n "${BREWFS_COMPACT_LIGHT_ENABLED:-}" \
            || -n "${BREWFS_COMPACT_LIGHT_THRESHOLD:-}" \
            || -n "${BREWFS_COMPACT_HEAVY_ENABLED:-}" \
            || -n "${BREWFS_COMPACT_HEAVY_FRAGMENT_THRESHOLD:-}" \
            || -n "${BREWFS_COMPACT_HEAVY_SLICE_THRESHOLD:-}" \
            || -n "${BREWFS_COMPACT_HEAVY_FORCE_FRAGMENT_THRESHOLD:-}" \
            || -n "${BREWFS_COMPACT_LOCK_ASYNC_TTL_SECS:-}" \
            || -n "${BREWFS_COMPACT_LOCK_SYNC_TTL_SECS:-}" \
            || -n "${BREWFS_COMPACT_LOCK_TTL_PER_SLICE_MS:-}" \
            || -n "${BREWFS_COMPACT_LOCK_MIN_TTL_SECS:-}" \
            || -n "${BREWFS_COMPACT_LOCK_MAX_TTL_SECS:-}" ]]; then
            echo
            echo "compact:"
            [[ -n "${BREWFS_COMPACT_MIN_SLICE_COUNT:-}" ]] && echo "  min_slice_count: ${BREWFS_COMPACT_MIN_SLICE_COUNT}"
            [[ -n "${BREWFS_COMPACT_MIN_FRAGMENT_RATIO:-}" ]] && echo "  min_fragment_ratio: ${BREWFS_COMPACT_MIN_FRAGMENT_RATIO}"
            [[ -n "${BREWFS_COMPACT_ASYNC_THRESHOLD:-}" ]] && echo "  async_threshold: ${BREWFS_COMPACT_ASYNC_THRESHOLD}"
            [[ -n "${BREWFS_COMPACT_SYNC_THRESHOLD:-}" ]] && echo "  sync_threshold: ${BREWFS_COMPACT_SYNC_THRESHOLD}"
            if [[ -n "${BREWFS_COMPACT_INTERVAL_SECS:-}" ]]; then
                echo "  interval:"
                echo "    secs: ${BREWFS_COMPACT_INTERVAL_SECS}"
                echo "    nanos: 0"
            fi
            [[ -n "${BREWFS_COMPACT_MAX_CHUNKS_PER_RUN:-}" ]] && echo "  max_chunks_per_run: ${BREWFS_COMPACT_MAX_CHUNKS_PER_RUN}"
            [[ -n "${BREWFS_COMPACT_MAX_CONCURRENT_TASKS:-}" ]] && echo "  max_concurrent_tasks: ${BREWFS_COMPACT_MAX_CONCURRENT_TASKS}"
            [[ -n "${BREWFS_COMPACT_LIGHT_ENABLED:-}" ]] && echo "  light_enabled: ${BREWFS_COMPACT_LIGHT_ENABLED}"
            [[ -n "${BREWFS_COMPACT_LIGHT_THRESHOLD:-}" ]] && echo "  light_threshold: ${BREWFS_COMPACT_LIGHT_THRESHOLD}"
            [[ -n "${BREWFS_COMPACT_HEAVY_ENABLED:-}" ]] && echo "  heavy_enabled: ${BREWFS_COMPACT_HEAVY_ENABLED}"
            [[ -n "${BREWFS_COMPACT_HEAVY_FRAGMENT_THRESHOLD:-}" ]] && echo "  heavy_fragment_threshold: ${BREWFS_COMPACT_HEAVY_FRAGMENT_THRESHOLD}"
            [[ -n "${BREWFS_COMPACT_HEAVY_SLICE_THRESHOLD:-}" ]] && echo "  heavy_slice_threshold: ${BREWFS_COMPACT_HEAVY_SLICE_THRESHOLD}"
            [[ -n "${BREWFS_COMPACT_HEAVY_FORCE_FRAGMENT_THRESHOLD:-}" ]] && echo "  heavy_force_fragment_threshold: ${BREWFS_COMPACT_HEAVY_FORCE_FRAGMENT_THRESHOLD}"
            if [[ -n "${BREWFS_COMPACT_LOCK_ASYNC_TTL_SECS:-}" \
                || -n "${BREWFS_COMPACT_LOCK_SYNC_TTL_SECS:-}" \
                || -n "${BREWFS_COMPACT_LOCK_TTL_PER_SLICE_MS:-}" \
                || -n "${BREWFS_COMPACT_LOCK_MIN_TTL_SECS:-}" \
                || -n "${BREWFS_COMPACT_LOCK_MAX_TTL_SECS:-}" ]]; then
                echo "  lock_ttl:"
                [[ -n "${BREWFS_COMPACT_LOCK_ASYNC_TTL_SECS:-}" ]] && echo "    async_ttl_secs: ${BREWFS_COMPACT_LOCK_ASYNC_TTL_SECS}"
                [[ -n "${BREWFS_COMPACT_LOCK_SYNC_TTL_SECS:-}" ]] && echo "    sync_ttl_secs: ${BREWFS_COMPACT_LOCK_SYNC_TTL_SECS}"
                [[ -n "${BREWFS_COMPACT_LOCK_TTL_PER_SLICE_MS:-}" ]] && echo "    ttl_per_slice_ms: ${BREWFS_COMPACT_LOCK_TTL_PER_SLICE_MS}"
                [[ -n "${BREWFS_COMPACT_LOCK_MIN_TTL_SECS:-}" ]] && echo "    min_ttl_secs: ${BREWFS_COMPACT_LOCK_MIN_TTL_SECS}"
                [[ -n "${BREWFS_COMPACT_LOCK_MAX_TTL_SECS:-}" ]] && echo "    max_ttl_secs: ${BREWFS_COMPACT_LOCK_MAX_TTL_SECS}"
            fi
        fi

        echo
        cat <<EOF
layout:
  chunk_size: ${BREWFS_CHUNK_SIZE:-67108864}
  block_size: ${BREWFS_BLOCK_SIZE:-4194304}
EOF

        if [[ -n "${BREWFS_FUSE_WORKERS:-}" || -n "${BREWFS_FUSE_MAX_BACKGROUND:-}" ]]; then
            echo
            echo "fuse:"
            if [[ -n "${BREWFS_FUSE_WORKERS:-}" ]]; then
                echo "  workers: ${BREWFS_FUSE_WORKERS}"
            fi
            if [[ -n "${BREWFS_FUSE_MAX_BACKGROUND:-}" ]]; then
                echo "  max_background: ${BREWFS_FUSE_MAX_BACKGROUND}"
            fi
        fi

        # Cache section (compression, writeback mode, memory/cache budgets, etc.)
        local comp="${BREWFS_COMPRESSION:-none}"
        local writeback_mode="${BREWFS_WRITEBACK_MODE:-}"
        if [[ -n "${BREWFS_CACHE_ROOT:-}" \
            || -n "${BREWFS_READ_MEMORY_BYTES:-}" \
            || -n "${BREWFS_READ_SSD_BYTES:-}" \
            || -n "${BREWFS_WRITE_MEMORY_BYTES:-}" \
            || -n "${BREWFS_WRITE_SSD_BYTES:-}" \
            || -n "${BREWFS_DIRTY_SLICE_TARGET_SIZE:-}" \
            || -n "${BREWFS_DIRTY_SLICE_MAX_AGE_MS:-}" \
            || -n "${BREWFS_UPLOAD_CONCURRENCY:-}" \
            || -n "${BREWFS_PREFETCH_ENABLED:-}" \
            || -n "${BREWFS_PREFETCH_MAX_BYTES:-}" \
            || -n "${BREWFS_PREFETCH_CONCURRENCY:-}" \
            || -n "${BREWFS_RANGE_BACKGROUND_PREFETCH:-}" \
            || -n "${BREWFS_MEMORY_BUDGET_BYTES:-}" \
            || -n "${BREWFS_COMPRESSION:-}" \
            || -n "${BREWFS_VERIFY_CACHE_CHECKSUM:-}" \
            || -n "${BREWFS_WRITEBACK_PERSIST_SYNC:-}" \
            || -n "${BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD:-}" \
            || -n "${BREWFS_UPLOAD_LIMIT_MIBPS:-}" \
            || -n "${BREWFS_DOWNLOAD_LIMIT_MIBPS:-}" \
            || -n "$comp" \
            || -n "$writeback_mode" ]]; then
            echo
            echo "cache:"
            [[ -n "${BREWFS_CACHE_ROOT:-}" ]] && echo "  root: ${BREWFS_CACHE_ROOT}"
            [[ -n "${BREWFS_READ_MEMORY_BYTES:-}" ]] && echo "  read_memory_bytes: ${BREWFS_READ_MEMORY_BYTES}"
            [[ -n "${BREWFS_READ_SSD_BYTES:-}" ]] && echo "  read_ssd_bytes: ${BREWFS_READ_SSD_BYTES}"
            [[ -n "${BREWFS_WRITE_MEMORY_BYTES:-}" ]] && echo "  write_memory_bytes: ${BREWFS_WRITE_MEMORY_BYTES}"
            [[ -n "${BREWFS_WRITE_SSD_BYTES:-}" ]] && echo "  write_ssd_bytes: ${BREWFS_WRITE_SSD_BYTES}"
            [[ -n "${BREWFS_DIRTY_SLICE_TARGET_SIZE:-}" ]] && echo "  dirty_slice_target_size: ${BREWFS_DIRTY_SLICE_TARGET_SIZE}"
            [[ -n "${BREWFS_DIRTY_SLICE_MAX_AGE_MS:-}" ]] && echo "  dirty_slice_max_age_ms: ${BREWFS_DIRTY_SLICE_MAX_AGE_MS}"
            [[ -n "${BREWFS_UPLOAD_CONCURRENCY:-}" ]] && echo "  upload_concurrency: ${BREWFS_UPLOAD_CONCURRENCY}"
            [[ -n "${BREWFS_PREFETCH_ENABLED:-}" ]] && echo "  prefetch_enabled: ${BREWFS_PREFETCH_ENABLED}"
            [[ -n "${BREWFS_PREFETCH_MAX_BYTES:-}" ]] && echo "  prefetch_max_bytes: ${BREWFS_PREFETCH_MAX_BYTES}"
            [[ -n "${BREWFS_PREFETCH_CONCURRENCY:-}" ]] && echo "  prefetch_concurrency: ${BREWFS_PREFETCH_CONCURRENCY}"
            [[ -n "${BREWFS_RANGE_BACKGROUND_PREFETCH:-}" ]] && echo "  range_background_prefetch: ${BREWFS_RANGE_BACKGROUND_PREFETCH}"
            [[ -n "${BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD:-}" ]] && echo "  populate_write_cache_after_upload: ${BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD}"
            [[ -n "${BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD:-}" ]] && echo "  persist_write_cache_after_upload: ${BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD}"
            [[ -n "${BREWFS_MEMORY_BUDGET_BYTES:-}" ]] && echo "  memory_budget_bytes: ${BREWFS_MEMORY_BUDGET_BYTES}"
            echo "  compression: ${comp}"
            [[ -n "${BREWFS_VERIFY_CACHE_CHECKSUM:-}" ]] && echo "  verify_cache_checksum: ${BREWFS_VERIFY_CACHE_CHECKSUM}"
            [[ -n "${BREWFS_WRITEBACK_PERSIST_SYNC:-}" ]] && echo "  writeback_persist_sync: ${BREWFS_WRITEBACK_PERSIST_SYNC}"
            [[ -n "${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES:-}" ]] && echo "  writeback_recent_pending_soft_bytes: ${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES}"
            [[ -n "${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES:-}" ]] && echo "  writeback_recent_pending_hard_bytes: ${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES}"
            if [[ -n "$writeback_mode" ]]; then
                echo "  writeback_mode: ${writeback_mode}"
            fi
            if [[ -n "${BREWFS_UPLOAD_LIMIT_MIBPS:-}" || -n "${BREWFS_DOWNLOAD_LIMIT_MIBPS:-}" ]]; then
                echo "  bandwidth:"
                if [[ -n "${BREWFS_UPLOAD_LIMIT_MIBPS:-}" ]]; then
                    echo "    upload_limit_mibps: ${BREWFS_UPLOAD_LIMIT_MIBPS}"
                fi
                if [[ -n "${BREWFS_DOWNLOAD_LIMIT_MIBPS:-}" ]]; then
                    echo "    download_limit_mibps: ${BREWFS_DOWNLOAD_LIMIT_MIBPS}"
                fi
            fi
        fi
    } >"$config_path"
}

install_mount_helper() {
    local helper="/usr/sbin/mount.fuse.brewfs"
    cat >"$helper" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH"

src="${1:-}"
target="${2:-}"
shift 2 || true

config_path="${BREWFS_CONFIG_PATH:-/run/brewfs/config.yaml}"
log_file="${BREWFS_LOG_FILE:-/artifacts/brewfs.log}"

mkdir -p "$target" "$(dirname "$log_file")"

is_brewfs_mounted() {
    findmnt -rn --target "$target" --output FSTYPE 2>/dev/null | grep -Eq '^fuse(\.|$)'
}

pre_wait_secs="${BREWFS_PRE_MOUNT_WAIT_SECS:-10}"
pre_deadline=$((SECONDS + pre_wait_secs))
while is_brewfs_mounted; do
    if (( SECONDS >= pre_deadline )); then
        echo "target $target is still mounted before starting BrewFS after ${pre_wait_secs}s" >&2
        exit 1
    fi
    sleep 0.1
done

EOF

    append_env_export() {
        local name="$1"
        local default="${2-}"
        local value="${!name:-$default}"
        if [[ -n "$value" || $# -ge 2 ]]; then
            printf 'export %s=%q\n' "$name" "$value" >>"$helper"
        fi
    }
    append_env_export AWS_ACCESS_KEY_ID "rustfsadmin"
    append_env_export AWS_SECRET_ACCESS_KEY "rustfsadmin"
    append_env_export AWS_DEFAULT_REGION "us-east-1"
    append_env_export AWS_EC2_METADATA_DISABLED "true"
    append_env_export AWS_SESSION_TOKEN
    append_env_export BREWFS_MOUNT_WAIT_SECS "60"
    append_env_export BREWFS_PRE_MOUNT_WAIT_SECS "10"
    append_env_export BREWFS_MOUNT_READY_DELAY_SECS "1"
    append_env_export BREWFS_NOFILE_LIMIT "1048576"
    append_env_export BREWFS_FUSE_WRITEBACK
    append_env_export BREWFS_FUSE_READ_DIRECT_IO
    append_env_export BREWFS_WRITEBACK_UPLOAD_CONCURRENCY
    append_env_export BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES
    append_env_export BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES
    append_env_export BREWFS_CACHED_SUB_BLOCK_IDLE_GRACE_MS
    append_env_export BREWFS_CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE_MS
    append_env_export PERF_FUSE_OPS_LOG "0"
    append_env_export BREWFS_FUSE_OP_LOG "0"
    append_env_export BREWFS_FUSE_LOG_FILE
    append_env_export BREWFS_VFS_TIMING "0"
    append_env_export RUST_LOG

    cat >>"$helper" <<'EOF'
if [[ -n "${BREWFS_NOFILE_LIMIT:-}" ]]; then
    ulimit -n "$BREWFS_NOFILE_LIMIT" >/dev/null 2>&1 \
        || echo "failed to raise nofile limit to $BREWFS_NOFILE_LIMIT; current=$(ulimit -n)" >&2
fi

# Enable FUSE op tracing when requested for detailed profiling.
if [[ "${PERF_FUSE_OPS_LOG:-0}" == "1" || "${BREWFS_FUSE_OP_LOG:-0}" == "1" ]]; then
    export RUST_LOG="${RUST_LOG:-brewfs=info,asyncfuse::raw::logfs=debug}"
else
    export RUST_LOG="${RUST_LOG:-error}"
fi

/usr/local/bin/brewfs mount --privileged --config "$config_path" "$target" >>"$log_file" 2>&1 &
brewfs_pid=$!

wait_secs="${BREWFS_MOUNT_WAIT_SECS:-60}"
deadline=$((SECONDS + wait_secs))
while (( SECONDS < deadline )); do
    if is_brewfs_mounted; then
        sleep "${BREWFS_MOUNT_READY_DELAY_SECS:-1}"
        exit 0
    fi
    if ! kill -0 "$brewfs_pid" 2>/dev/null; then
        status=0
        wait "$brewfs_pid" || status=$?
        if (( status == 0 )); then
            status=1
        fi
        echo "brewfs mount process exited before $target became a mountpoint (status=$status)" >&2
        exit "$status"
    fi
    sleep 0.1
done

echo "timed out after ${wait_secs}s waiting for BrewFS mount at $target" >&2
echo "brewfs_pid=$brewfs_pid running=$(kill -0 "$brewfs_pid" 2>/dev/null && echo yes || echo no)" >&2
echo "findmnt: $(findmnt -rn --target "$target" --output TARGET,FSTYPE,SOURCE 2>/dev/null || true)" >&2
ps -o pid,ppid,stat,etime,comm,args -p "$brewfs_pid" >&2 || true
exit 1
EOF

    chmod +x "$helper"
}

prepare_artifacts() {
    mkdir -p "$artifact_dir/results" "$artifact_dir/tools" "$artifact_dir/diagnostics"
    touch "$artifact_dir/perf.log" "$artifact_dir/perf-summary.tsv" "$artifact_dir/report.md" >/dev/null 2>&1 || true
    printf 'tool\tstatus\tseconds\tlog\n' >"$artifact_dir/perf-summary.tsv"
    printf 'tool\tpost_fio_drain_s\tpending_bytes\tdirty_bytes\tbuffer_dirty_bytes\n' \
        >"$artifact_dir/post-write-drain.tsv"
    printf 'ts\ttool\telapsed_s\tbuffer_dirty_bytes\tlive_dirty_bytes\tlive_slices\trecent_pending_upload_bytes\trecent_uploaded_bytes\tstage_inflight_bytes\tremote_upload_inflight_bytes\ts3_put_ops\ts3_put_bytes\tbuffer_soft_sleep_ops\tbuffer_moderate_sleep_ops\tbuffer_hard_sleep_ops\tfuse_write_bytes\tupload_batch_ops\n' \
        >"$artifact_dir/writeback-samples.tsv"
    write_perf_profile
    if truthy_env "${PERF_FUSE_OPS_LOG:-0}" || truthy_env "${BREWFS_FUSE_OP_LOG:-0}"; then
        export BREWFS_FUSE_OP_LOG=1
        export BREWFS_FUSE_LOG_FILE="$artifact_dir/brewfs_fuse_ops.log"
    else
        export BREWFS_FUSE_OP_LOG=0
        unset BREWFS_FUSE_LOG_FILE || true
    fi
}

copy_artifacts() {
    mkdir -p "$artifact_dir"
    if [[ -f "$log_file" && "$log_file" != "$artifact_dir/brewfs.log" ]]; then
        cp -f "$log_file" "$artifact_dir/brewfs.log" || true
    fi
    if [[ -n "${BREWFS_FUSE_LOG_FILE:-}" && -f "${BREWFS_FUSE_LOG_FILE}" && "${BREWFS_FUSE_LOG_FILE}" != "$artifact_dir/brewfs_fuse_ops.log" ]]; then
        cp -f "${BREWFS_FUSE_LOG_FILE}" "$artifact_dir/brewfs_fuse_ops.log" || true
    fi
    if [[ -f "$config_path" ]]; then
        cp -f "$config_path" "$artifact_dir/backend.yml" || true
    fi
    chmod -R a+rwX "$artifact_dir" >/dev/null 2>&1 || true
}

cleanup() {
    while findmnt -rn --target "$mount_dir" --output FSTYPE 2>/dev/null | grep -Eq '^fuse(\.|$)'; do
        fusermount3 -u "$mount_dir" >/dev/null 2>&1 \
            || umount -f "$mount_dir" >/dev/null 2>&1 \
            || umount -l "$mount_dir" >/dev/null 2>&1 \
            || sleep 1
    done
    local pids
    pids="$(ps -eo pid=,args= | awk '$0 ~ /\/usr\/local\/bin\/brewfs mount/ && $0 !~ /awk/ {print $1}')" || true
    if [[ -n "$pids" ]]; then
        while read -r pid; do
            [[ -n "$pid" ]] && kill "$pid" >/dev/null 2>&1 || true
        done <<<"$pids"
    fi
}

on_exit() {
    local status=$?
    copy_artifacts || true
    cleanup || true
    trap - EXIT
    exit "$status"
}

require_tool_bin() {
    local bin="$1"
    if [[ ! -x "$bin" ]]; then
        err "找不到可执行工具: $bin"
        exit 1
    fi
}

run_metadata_fallback() {
    local tool="$1"
    local work_dir="$2"
    local fallback="${PERF_METADATA_FALLBACK_BIN:-/usr/local/bin/perf_metadata_fallback.py}"

    require_tool_bin "$fallback"
    rm -rf "$work_dir"
    mkdir -p "$work_dir"
    info "使用 metadata fallback: $tool ($fallback)"
    run_logged_tool "$tool" python3 "$fallback" "$tool" "$work_dir"
}

redis_diag_enabled() {
    [[ "$meta_backend" == "redis" ]] || return 1
    [[ -n "$meta_url" ]] || return 1
    command -v redis-cli >/dev/null 2>&1 || return 1
}

redis_diag_cli() {
    redis-cli -u "$meta_url" "$@"
}

redis_diag_before_tool() {
    local tool="$1"
    redis_diag_enabled || return 0

    {
        echo "# Redis diagnostic reset before $tool"
        date -Iseconds
        redis_diag_cli CONFIG SET latency-monitor-threshold "${PERF_REDIS_LATENCY_THRESHOLD_MS:-1}" || true
        redis_diag_cli CONFIG RESETSTAT || true
        redis_diag_cli SLOWLOG RESET || true
        redis_diag_cli LATENCY RESET || true
    } >"$artifact_dir/diagnostics/redis-${tool}-before.txt" 2>&1 || true
}

redis_diag_after_tool() {
    local tool="$1"
    redis_diag_enabled || return 0

    {
        echo "# Redis diagnostics after $tool"
        date -Iseconds
        echo
        echo "## INFO commandstats"
        redis_diag_cli INFO commandstats || true
        echo
        echo "## INFO stats"
        redis_diag_cli INFO stats || true
        echo
        echo "## SLOWLOG GET 20"
        redis_diag_cli SLOWLOG GET 20 || true
        echo
        echo "## LATENCY LATEST"
        redis_diag_cli LATENCY LATEST || true
        echo
        echo "## LATENCY DOCTOR"
        redis_diag_cli LATENCY DOCTOR || true
    } >"$artifact_dir/diagnostics/redis-${tool}-after.txt" 2>&1 || true
}

stats_snapshot_tool() {
    local tool="$1"
    local phase="$2"
    local stats_path="$mount_dir/.stats"
    {
        date -Iseconds
        echo
        if ! brewfs_stats_dump; then
            echo "missing_or_unavailable $stats_path timeout=${PERF_STATS_READ_TIMEOUT_SECS:-5s}"
        fi
    } >"$artifact_dir/diagnostics/stats-${tool}-${phase}.txt" 2>&1 || true
}

stats_snapshot_before_tool() {
    stats_snapshot_tool "$1" before
}

stats_snapshot_after_tool() {
    stats_snapshot_tool "$1" after
}

brewfs_stat_value() {
    local metric="$1"
    brewfs_stats_dump | awk -v metric="$metric" '
        $1 == metric {
            print $2
            found = 1
            exit
        }
        END {
            if (!found) {
                exit 1
            }
        }
    '
}

brewfs_stats_dump() {
    local stats_path="$mount_dir/.stats"
    local timeout_value="${PERF_STATS_READ_TIMEOUT_SECS:-5s}"
    timeout --foreground "$timeout_value" cat "$stats_path" | tr -d '\000'
}

numeric_stat_or_zero() {
    local metric="$1"
    local value
    value="$(brewfs_stat_value "$metric" 2>/dev/null || true)"
    if [[ "$value" =~ ^[0-9]+$ ]]; then
        printf '%s' "$value"
    else
        printf '0'
    fi
}

start_writeback_sampler() {
    local tool="$1"
    local interval="${PERF_WRITEBACK_SAMPLE_INTERVAL_SECS:-1}"
    local sample_path="$artifact_dir/writeback-samples.tsv"
    local start_ts

    WRITEBACK_SAMPLER_PID=""
    truthy_env "${PERF_WRITEBACK_SAMPLER:-true}" || return 0

    start_ts="$(date +%s)"
    (
        while true; do
            local now elapsed
            now="$(date +%s)"
            elapsed="$((now - start_ts))"
            printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                "$(date -Iseconds)" \
                "$tool" \
                "$elapsed" \
                "$(numeric_stat_or_zero brewfs_writeback_dirty_bytes)" \
                "$(numeric_stat_or_zero brewfs_writeback_live_dirty_bytes)" \
                "$(numeric_stat_or_zero brewfs_writeback_live_slices)" \
                "$(numeric_stat_or_zero brewfs_writeback_recent_pending_upload_bytes)" \
                "$(numeric_stat_or_zero brewfs_writeback_recent_uploaded_bytes)" \
                "$(numeric_stat_or_zero brewfs_writeback_stage_inflight_bytes)" \
                "$(numeric_stat_or_zero brewfs_writeback_remote_upload_inflight_bytes)" \
                "$(numeric_stat_or_zero brewfs_s3_put_ops_total)" \
                "$(numeric_stat_or_zero brewfs_s3_put_bytes_total)" \
                "$(numeric_stat_or_zero brewfs_writeback_buffer_soft_sleep_ops)" \
                "$(numeric_stat_or_zero brewfs_writeback_buffer_moderate_sleep_ops)" \
                "$(numeric_stat_or_zero brewfs_writeback_buffer_hard_sleep_ops)" \
                "$(numeric_stat_or_zero brewfs_fuse_write_bytes_total)" \
                "$(numeric_stat_or_zero brewfs_writeback_upload_batch_ops_total)" \
                >>"$sample_path"
            sleep "$interval"
        done
    ) &
    WRITEBACK_SAMPLER_PID="$!"
}

stop_writeback_sampler() {
    local pid="${1:-}"
    [[ -n "$pid" ]] || return 0
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
}

max_u64() {
    local lhs="$1"
    local rhs="$2"
    if (( lhs >= rhs )); then
        printf '%s' "$lhs"
    else
        printf '%s' "$rhs"
    fi
}

wait_for_fio_prefill_drain() {
    local tool="$1"
    local timeout="${PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS:-600}"
    local interval="${PERF_FIO_PREFILL_DRAIN_INTERVAL_SECS:-2}"
    local threshold="${PERF_FIO_PREFILL_DRAIN_PENDING_BYTES:-0}"
    local start now elapsed pending dirty buffer_dirty drain_bytes put_bytes uploaded

    info "等待 fio 预填充写回完成: $tool (threshold=${threshold} bytes, timeout=${timeout}s)"
    stats_snapshot_before_tool "${tool}-prefill-drained"
    stats_snapshot_before_tool "${tool}-prefill-drain-timeout"
    start="$(date +%s)"

    while true; do
        pending="$(numeric_stat_or_zero brewfs_writeback_recent_pending_upload_bytes)"
        dirty="$(numeric_stat_or_zero brewfs_writeback_dirty_bytes)"
        buffer_dirty="$(numeric_stat_or_zero brewfs_buffer_dirty_bytes)"
        put_bytes="$(numeric_stat_or_zero brewfs_s3_put_bytes_total)"
        uploaded="$(numeric_stat_or_zero brewfs_writeback_recent_uploaded_bytes)"
        drain_bytes="$(max_u64 "$pending" "$dirty")"
        drain_bytes="$(max_u64 "$drain_bytes" "$buffer_dirty")"

        now="$(date +%s)"
        elapsed="$((now - start))"

        if (( drain_bytes <= threshold )); then
            ok "fio 预填充写回已完成: $tool (pending=$pending dirty=$dirty buffer_dirty=$buffer_dirty put_bytes=$put_bytes uploaded=$uploaded elapsed=${elapsed}s)"
            stats_snapshot_after_tool "${tool}-prefill-drained"
            return 0
        fi

        if (( elapsed >= timeout )); then
            err "fio 预填充写回等待超时: $tool (pending=$pending dirty=$dirty buffer_dirty=$buffer_dirty put_bytes=$put_bytes uploaded=$uploaded elapsed=${elapsed}s)"
            stats_snapshot_after_tool "${tool}-prefill-drain-timeout"
            return 1
        fi

        if (( elapsed % 10 == 0 )); then
            info "  写回等待中: pending=$pending dirty=$dirty buffer_dirty=$buffer_dirty put_bytes=$put_bytes uploaded=$uploaded elapsed=${elapsed}s"
        fi
        sleep "$interval"
    done
}

wait_for_fio_post_write_drain() {
    local tool="$1"
    local timeout="${PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS:-600}"
    local interval="${PERF_FIO_POST_WRITE_DRAIN_INTERVAL_SECS:-2}"
    local threshold="${PERF_FIO_POST_WRITE_DRAIN_PENDING_BYTES:-0}"
    local start now elapsed pending dirty buffer_dirty drain_bytes

    truthy_env "${PERF_FIO_POST_WRITE_DRAIN:-false}" || return 0
    case "$tool" in
        fio-seqwrite*|fio-randwrite*|fio-randrw*|fio-bigwrite*) ;;
        *) return 0 ;;
    esac

    info "等待 fio 写入后写回完成: $tool (threshold=${threshold} bytes, timeout=${timeout}s)"
    stats_snapshot_before_tool "${tool}-post-write-drained"
    stats_snapshot_before_tool "${tool}-post-write-drain-timeout"
    start="$(date +%s)"

    while true; do
        pending="$(numeric_stat_or_zero brewfs_writeback_recent_pending_upload_bytes)"
        dirty="$(numeric_stat_or_zero brewfs_writeback_dirty_bytes)"
        buffer_dirty="$(numeric_stat_or_zero brewfs_buffer_dirty_bytes)"
        drain_bytes="$(max_u64 "$pending" "$dirty")"
        drain_bytes="$(max_u64 "$drain_bytes" "$buffer_dirty")"

        now="$(date +%s)"
        elapsed="$((now - start))"

        if (( drain_bytes <= threshold )); then
            ok "fio 写入后写回已完成: $tool (pending=$pending dirty=$dirty buffer_dirty=$buffer_dirty elapsed=${elapsed}s)"
            printf '%s\t%s\t%s\t%s\t%s\n' "$tool" "$elapsed" "$pending" "$dirty" "$buffer_dirty" \
                >>"$artifact_dir/post-write-drain.tsv"
            stats_snapshot_after_tool "${tool}-post-write-drained"
            return 0
        fi

        if (( elapsed >= timeout )); then
            err "fio 写入后写回等待超时: $tool (pending=$pending dirty=$dirty buffer_dirty=$buffer_dirty elapsed=${elapsed}s)"
            printf '%s\ttimeout:%s\t%s\t%s\t%s\n' "$tool" "$elapsed" "$pending" "$dirty" "$buffer_dirty" \
                >>"$artifact_dir/post-write-drain.tsv"
            stats_snapshot_after_tool "${tool}-post-write-drain-timeout"
            return 1
        fi

        if (( elapsed % 10 == 0 )); then
            info "  写后写回等待中: pending=$pending dirty=$dirty buffer_dirty=$buffer_dirty elapsed=${elapsed}s"
        fi
        sleep "$interval"
    done
}

drop_kernel_page_cache_if_requested() {
    if truthy_env "${PERF_FIO_DROP_CACHES:-false}" || truthy_env "${PERF_FIO_COLD_READ_DROP_CACHES:-false}"; then
        info "请求 drop_caches 以降低页缓存影响"
        sync || true
        if ! sh -c 'echo 3 > /proc/sys/vm/drop_caches' >/dev/null 2>&1; then
            err "drop_caches 失败；继续测试，但结果可能仍受页缓存影响"
        fi
    fi
}

clear_brewfs_cache_root_if_requested() {
    if truthy_env "${PERF_FIO_COLD_READ:-false}" || truthy_env "${PERF_FIO_COLD_READ_CLEAR_CACHE:-false}"; then
        local root="${BREWFS_CACHE_ROOT:-${XDG_CACHE_HOME:-/root/.cache}/brewfs}"
        if [[ -n "$root" && "$root" == /* && "$root" != "/" ]]; then
            info "清理 BrewFS 本地 cache root: $root"
            rm -rf -- "$root"
        else
            err "跳过 BrewFS cache root 清理，路径不安全: ${root:-<empty>}"
        fi
    fi
}

remount_brewfs_for_fio_profile() {
    local tool="$1"

    info "为 fio cold-read 重挂载 BrewFS: $tool"
    cleanup
    clear_brewfs_cache_root_if_requested
    drop_kernel_page_cache_if_requested
    mount_brewfs
}

mount_brewfs() {
    mkdir -p "$mount_dir"
    if findmnt -rn --target "$mount_dir" --output FSTYPE 2>/dev/null | grep -Eq '^fuse(\.|$)'; then
        cleanup
    fi

    info "挂载 BrewFS: $mount_dir"
    mount -t fuse.brewfs brewfs "$mount_dir"

    local i=0
    for ((i = 0; i < 15; i++)); do
        if findmnt -rn --target "$mount_dir" --output FSTYPE 2>/dev/null | grep -Eq '^fuse(\.|$)'; then
            ok "BrewFS 已挂载"
            return 0
        fi
        sleep 1
    done

    err "BrewFS 挂载失败: $mount_dir"
    exit 1
}

run_logged_tool() {
    local tool="$1"
    shift
    local log_path="$artifact_dir/tools/${tool}.log"
    local start end elapsed status sampler_pid

    start="$(date +%s)"
    info "运行压力工具: $tool"
    info "  命令: $*"
    redis_diag_before_tool "$tool"
    start_writeback_sampler "$tool" || true
    sampler_pid="${WRITEBACK_SAMPLER_PID:-}"
    set +e
    if [[ "${PERF_LOG_TO_CONSOLE:-false}" == "true" ]]; then
        "$@" 2>&1 | tee "$log_path"
        status="${PIPESTATUS[0]}"
    else
        "$@" >"$log_path" 2>&1
        status=$?
    fi
    set -e
    stop_writeback_sampler "$sampler_pid"
    end="$(date +%s)"
    elapsed="$((end - start))"
    stats_snapshot_after_tool "$tool"
    redis_diag_after_tool "$tool"

    local log_size
    log_size=$(wc -c < "$log_path" 2>/dev/null || echo 0)

    if [[ "$status" -eq 0 ]]; then
        ok "压力工具完成: $tool (${elapsed}s, log=${log_size} bytes)"
        printf '%s\tpass\t%s\t%s\n' "$tool" "$elapsed" "$log_path" >>"$artifact_dir/perf-summary.tsv"
    else
        err "压力工具失败: $tool (exit=$status, ${elapsed}s, log=${log_size} bytes)"
        printf '%s\tfail(%s)\t%s\t%s\n' "$tool" "$status" "$elapsed" "$log_path" >>"$artifact_dir/perf-summary.tsv"
        # Show last 5 non-empty lines of the log to help diagnose failures
        if [[ -s "$log_path" ]]; then
            err "  最后几行日志:"
            grep -v '^$' "$log_path" | tail -5 | while read -r line; do
                err "    $line"
            done
        fi
    fi

    return "$status"
}

run_dirstress() {
    local bin="$xfstests_dir/src/dirstress"
    local work_dir="$mount_dir/.perf-dirstress"
    local -a args=()

    if [[ ! -x "$bin" ]]; then
        run_metadata_fallback dirstress "$work_dir"
        return
    fi
    rm -rf "$work_dir"
    mkdir -p "$work_dir"

    if [[ -n "${PERF_DIRSTRESS_ARGS:-}" ]]; then
        read -r -a args <<<"${PERF_DIRSTRESS_ARGS}"
    else
        args=(
            -d "$work_dir"
            -p "${PERF_DIRSTRESS_PROCS:-4}"
            -f "${PERF_DIRSTRESS_FILES:-200}"
            -n "${PERF_DIRSTRESS_PROCS_PER_DIR:-2}"
            -s "${PERF_DIRSTRESS_SEED:-1}"
        )
    fi

    run_logged_tool dirstress "$bin" "${args[@]}"

    # Summarize dirstress errors (File exists errors are expected under concurrency)
    local dirstress_log="$artifact_dir/tools/dirstress.log"
    if [[ -f "$dirstress_log" ]]; then
        local total_errs mkdir_errs symlink_errs mknod_errs
        total_errs=$(grep -c '!!' "$dirstress_log" 2>/dev/null || echo 0)
        mkdir_errs=$(grep -c 'mkdir.*File exists' "$dirstress_log" 2>/dev/null || echo 0)
        symlink_errs=$(grep -c 'symlink.*File exists' "$dirstress_log" 2>/dev/null || echo 0)
        mknod_errs=$(grep -c "mknod.*Function not implemented" "$dirstress_log" 2>/dev/null || echo 0)
        info "dirstress 错误汇总: total=$total_errs mkdir_EEXIST=$mkdir_errs symlink_EEXIST=$symlink_errs mknod_ENOSYS=$mknod_errs"
    fi
}

run_dirperf() {
    local bin="$xfstests_dir/src/dirperf"
    local work_dir="$mount_dir/.perf-dirperf"
    local -a args=()

    if [[ ! -x "$bin" ]]; then
        run_metadata_fallback dirperf "$work_dir"
        return
    fi
    rm -rf "$work_dir"
    mkdir -p "$work_dir"

    if [[ -n "${PERF_DIRPERF_ARGS:-}" ]]; then
        read -r -a args <<<"${PERF_DIRPERF_ARGS}"
    else
        args=(
            -d "$work_dir"
            -a "${PERF_DIRPERF_ADDSTEP:-100}"
            -f "${PERF_DIRPERF_FIRST:-100}"
            -l "${PERF_DIRPERF_LAST:-1000}"
            -c "${PERF_DIRPERF_NAME_LEN:-16}"
            -n "${PERF_DIRPERF_DIRS:-2}"
            -s "${PERF_DIRPERF_STATS:-5}"
        )
    fi

    run_logged_tool dirperf "$bin" "${args[@]}"
}

run_metaperf() {
    local bin="$xfstests_dir/src/metaperf"
    local work_dir="$mount_dir/.perf-metaperf"
    local -a args=()

    if [[ ! -x "$bin" ]]; then
        run_metadata_fallback metaperf "$work_dir"
        return
    fi
    rm -rf "$work_dir"
    mkdir -p "$work_dir"

    if [[ -n "${PERF_METAPERF_ARGS:-}" ]]; then
        read -r -a args <<<"${PERF_METAPERF_ARGS}"
    else
        args=(
            -d "$work_dir"
            -t "${PERF_METAPERF_SECONDS:-30}"
            -s "${PERF_METAPERF_FILE_SIZE:-4096}"
            -l "${PERF_METAPERF_NAME_LEN:-16}"
            -L "${PERF_METAPERF_BG_NAME_LEN:-16}"
            -n "${PERF_METAPERF_OP_FILES:-200}"
            -N "${PERF_METAPERF_BG_FILES:-2000}"
            create
            open
            stat
            readdir
            rename
        )
    fi

    run_logged_tool metaperf "$bin" "${args[@]}"
}

run_looptest() {
    local bin="$xfstests_dir/src/looptest"
    local work_dir="$mount_dir/.perf-looptest"
    local loop_file="$work_dir/looptest.dat"
    local -a args=()

    if [[ ! -x "$bin" ]]; then
        run_metadata_fallback looptest "$work_dir"
        return
    fi
    rm -rf "$work_dir"
    mkdir -p "$work_dir"

    if [[ -n "${PERF_LOOPTEST_ARGS:-}" ]]; then
        read -r -a args <<<"${PERF_LOOPTEST_ARGS}"
    else
        args=(
            -i "${PERF_LOOPTEST_ITERS:-200}"
            -o
            -r
            -w
            -t
            -f
            -s
            -v
            -b "${PERF_LOOPTEST_BUF_SIZE:-1048576}"
            "$loop_file"
        )
    fi

    run_logged_tool looptest "$bin" "${args[@]}"

    # Post-validation: verify the test file was created and modified
    if [[ -f "$loop_file" ]]; then
        local looptest_size
        looptest_size=$(stat -c%s "$loop_file" 2>/dev/null || echo 0)
        info "looptest 测试文件: $loop_file (size=$looptest_size)"
    else
        err "looptest 未能创建测试文件: $loop_file"
    fi
}

append_fio_log_summary() {
    local json_path="$1"
    local log_path="$2"
    local label="${3:-fio}"

    if [[ -f "$json_path" ]] && command -v python3 >/dev/null 2>&1; then
        python3 -c "
import json, sys
with open('$json_path') as f:
    data = json.load(f)
jobs = data.get('jobs', [])
if not jobs:
    sys.exit(1)
read = jobs[0].get('read', {})
write = jobs[0].get('write', {})
opts = jobs[0].get('job options', {})
print(f\"${label}: {opts.get('rw','?')} bs={opts.get('bs','?')} size={opts.get('size','?')} numjobs={opts.get('numjobs','?')} runtime={opts.get('runtime','?')}s\")
print(f\"  read:  bw={read.get('bw','?')} KiB/s  iops={read.get('iops','?'):.1f}  lat_avg={read.get('clat_ns',{}).get('mean',0)/1e6:.2f}ms  lat_p99={read.get('clat_ns',{}).get('percentile',{}).get('99.000000',0)/1e6:.2f}ms\")
print(f\"  write: bw={write.get('bw','?')} KiB/s  iops={write.get('iops','?'):.1f}  lat_avg={write.get('clat_ns',{}).get('mean',0)/1e6:.2f}ms  lat_p99={write.get('clat_ns',{}).get('percentile',{}).get('99.000000',0)/1e6:.2f}ms\")
print(f\"  total: {read.get('io_bytes',0)+write.get('io_bytes',0)} bytes, {read.get('total_ios',0)+write.get('total_ios',0)} IOs\")
" >> "$log_path" 2>/dev/null || true
    fi
}

summarize_stress_ng_log() {
    local log_path="$artifact_dir/tools/stress-ng.log"
    local summary_path="$artifact_dir/tools/stress-ng-summary.tsv"

    [[ -f "$log_path" ]] || return 0

    awk '
        BEGIN {
            print "stressor\tbogo_ops\treal_secs\tusr_secs\tsys_secs\treal_ops_per_sec\tcpu_ops_per_sec"
        }
        $1 == "stress-ng:" && $2 == "metrc:" && $4 != "stressor" && $5 ~ /^[0-9]+$/ {
            printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", $4, $5, $6, $7, $8, $9, $10
        }
    ' "$log_path" >"$summary_path"

    if [[ "$(wc -l <"$summary_path" 2>/dev/null || echo 0)" -gt 1 ]]; then
        info "stress-ng 摘要: $summary_path"
    fi
}

run_stress_ng() {
    local work_dir="$mount_dir/.perf-stress-ng"
    local -a args=()
    local status=0

    if ! command -v stress-ng >/dev/null 2>&1; then
        err "缺少 stress-ng"
        return 1
    fi

    rm -rf "$work_dir"
    mkdir -p "$work_dir"

    if [[ -n "${PERF_STRESS_NG_ARGS:-}" ]]; then
        read -r -a args <<<"${PERF_STRESS_NG_ARGS}"
    else
        args=(
            --temp-path "$work_dir"
            --timeout "${PERF_STRESS_NG_TIMEOUT:-10s}"
            --metrics-brief
            --verify
            --dir "${PERF_STRESS_NG_DIR_WORKERS:-1}"
            --dir-ops "${PERF_STRESS_NG_DIR_OPS:-1000}"
            --dentry "${PERF_STRESS_NG_DENTRY_WORKERS:-1}"
            --dentry-ops "${PERF_STRESS_NG_DENTRY_OPS:-100}"
            --rename "${PERF_STRESS_NG_RENAME_WORKERS:-1}"
            --rename-ops "${PERF_STRESS_NG_RENAME_OPS:-1000}"
            --unlink "${PERF_STRESS_NG_UNLINK_WORKERS:-1}"
            --unlink-ops "${PERF_STRESS_NG_UNLINK_OPS:-500}"
            --hdd "${PERF_STRESS_NG_HDD_WORKERS:-1}"
            --hdd-bytes "${PERF_STRESS_NG_HDD_BYTES:-8M}"
            --hdd-write-size "${PERF_STRESS_NG_HDD_WRITE_SIZE:-128K}"
        )
    fi

    run_logged_tool stress-ng stress-ng "${args[@]}" || status=$?
    summarize_stress_ng_log
    return "$status"
}

prepare_fio_dataset() {
    local tool="$1"
    local work_dir="$2"
    local job_name="$3"
    local dataset_size="$4"
    local direct_mode="$5"
    local numjobs="$6"
    local bs="$7"
    local ioengine="$8"
    local iodepth="$9"
    local prep_log="$artifact_dir/tools/${tool}-prepare.log"
    local -a prep_args=(
        --name="$job_name"
        --directory="$work_dir"
        --rw=write
        --bs="${PERF_FIO_PREP_BS:-$bs}"
        --size="$dataset_size"
        --numjobs="${PERF_FIO_PREP_NUMJOBS:-$numjobs}"
        --ioengine="${PERF_FIO_PREP_IOENGINE:-$ioengine}"
        --iodepth="${PERF_FIO_PREP_IODEPTH:-$iodepth}"
        --direct="$direct_mode"
        --end_fsync=1
        --group_reporting
        --eta=never
    )

    info "预填充 fio 数据集: $tool"
    if [[ "${PERF_LOG_TO_CONSOLE:-false}" == "true" ]]; then
        fio "${prep_args[@]}" 2>&1 | tee "$prep_log"
        return "${PIPESTATUS[0]}"
    fi
    fio "${prep_args[@]}" >"$prep_log" 2>&1
}

run_fio_custom() {
    local work_dir="$mount_dir/.perf-fio"
    local json_path="$artifact_dir/results/fio.json"
    local -a args=()

    if ! command -v fio >/dev/null 2>&1; then
        err "找不到 fio"
        exit 1
    fi

    rm -rf "$work_dir"
    mkdir -p "$work_dir"

    if [[ -n "${PERF_FIO_ARGS:-}" ]]; then
        read -r -a args <<<"${PERF_FIO_ARGS}"
    else
        args=(
            --name="${PERF_FIO_NAME:-brewfs-randrw}"
            --directory="$work_dir"
            --rw="${PERF_FIO_RW:-randrw}"
            --rwmixread="${PERF_FIO_RWMIXREAD:-70}"
            --bs="${PERF_FIO_BS:-4m}"
            --size="${PERF_FIO_SIZE:-256m}"
            --numjobs="${PERF_FIO_NUMJOBS:-4}"
            --ioengine="${PERF_FIO_IOENGINE:-io_uring}"
            --iodepth="${PERF_FIO_IODEPTH:-1}"
            --direct="${PERF_FIO_DIRECT:-0}"
            --runtime="${PERF_FIO_RUNTIME:-60}"
            --time_based
            --group_reporting
            --eta=never
        )
    fi

    args+=(--output-format=json --output="$json_path")
    run_logged_tool fio fio "${args[@]}"
    append_fio_log_summary "$json_path" "$artifact_dir/tools/fio.log" "fio"
}

run_fio_profile() {
    local tool="$1"
    local mode="$2"
    local direct_override="${3:-}"
    local profile_key_override="${4:-}"
    local work_dir="$mount_dir/.perf-${tool}"
    local json_path="$artifact_dir/results/${tool}.json"
    local profile_suffix="${tool#fio-}"
    local profile_key
    local profile_args_var
    local name_var
    local rw_var
    local rwmixread_var
    local bs_var
    local size_var
    local numjobs_var
    local ioengine_var
    local iodepth_var
    local direct_var
    local runtime_var
    local name rw rwmixread bs size numjobs ioengine iodepth direct runtime
    local needs_prefill=false
    local use_time_based=true
    local use_end_fsync=false
    local use_refill_buffers=false
    local -a args=()

    if [[ -n "$profile_key_override" ]]; then
        profile_key="$profile_key_override"
    else
        profile_key="$(printf '%s' "$profile_suffix" | tr '[:lower:]-' '[:upper:]_')"
    fi
    profile_args_var="PERF_FIO_${profile_key}_ARGS"
    name_var="PERF_FIO_${profile_key}_NAME"
    rw_var="PERF_FIO_${profile_key}_RW"
    rwmixread_var="PERF_FIO_${profile_key}_RWMIXREAD"
    bs_var="PERF_FIO_${profile_key}_BS"
    size_var="PERF_FIO_${profile_key}_SIZE"
    numjobs_var="PERF_FIO_${profile_key}_NUMJOBS"
    ioengine_var="PERF_FIO_${profile_key}_IOENGINE"
    iodepth_var="PERF_FIO_${profile_key}_IODEPTH"
    direct_var="PERF_FIO_${profile_key}_DIRECT"
    runtime_var="PERF_FIO_${profile_key}_RUNTIME"

    local direct_matrix_var="PERF_FIO_${profile_key}_DIRECT_MATRIX"
    local direct_matrix="${!direct_matrix_var:-${PERF_FIO_DIRECT_MATRIX:-}}"
    if [[ -z "$direct_override" && -z "${!profile_args_var:-}" && -n "$direct_matrix" ]]; then
        local direct_value matrix_status=0
        for direct_value in $direct_matrix; do
            case "$direct_value" in
                0|1) ;;
                *)
                    err "无效的 fio direct matrix 值: $direct_value (只支持 0 或 1)"
                    return 1
                    ;;
            esac
            run_fio_profile "${tool}-direct${direct_value}" "$mode" "$direct_value" "$profile_key" || matrix_status=1
        done
        return "$matrix_status"
    fi

    rm -rf "$work_dir"
    mkdir -p "$work_dir"

    if [[ -n "${!profile_args_var:-}" ]]; then
        read -r -a args <<<"${!profile_args_var}"
    else
        case "$mode" in
            seqread)
                name="$(env_or_default "$name_var" PERF_FIO_NAME brewfs-seqread)"
                rw="$(env_or_default "$rw_var" PERF_FIO_RW read)"
                bs="$(env_or_default "$bs_var" PERF_FIO_BS 4m)"
                size="$(env_or_default "$size_var" PERF_FIO_SIZE 1g)"
                numjobs="$(env_or_default "$numjobs_var" PERF_FIO_NUMJOBS 1)"
                ioengine="$(env_or_default "$ioengine_var" PERF_FIO_IOENGINE io_uring)"
                iodepth="$(env_or_default "$iodepth_var" PERF_FIO_IODEPTH 1)"
                direct="${direct_override:-$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)}"
                runtime="$(env_or_default "$runtime_var" PERF_FIO_RUNTIME 60)"
                use_time_based=true
                use_end_fsync=false
                use_refill_buffers=false
                needs_prefill=true
                ;;
            seqwrite)
                name="$(env_or_default "$name_var" PERF_FIO_NAME brewfs-seqwrite)"
                rw="$(env_or_default "$rw_var" PERF_FIO_RW write)"
                bs="$(env_or_default "$bs_var" PERF_FIO_BS 4m)"
                size="$(env_or_default "$size_var" PERF_FIO_SIZE 1g)"
                numjobs="$(env_or_default "$numjobs_var" PERF_FIO_NUMJOBS 1)"
                ioengine="$(env_or_default "$ioengine_var" PERF_FIO_IOENGINE io_uring)"
                iodepth="$(env_or_default "$iodepth_var" PERF_FIO_IODEPTH 1)"
                direct="${direct_override:-$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)}"
                runtime="$(env_or_default "$runtime_var" PERF_FIO_RUNTIME 60)"
                use_time_based=true
                use_end_fsync=false
                use_refill_buffers=false
                ;;
            randread)
                name="$(env_or_default "$name_var" PERF_FIO_NAME brewfs-randread)"
                rw="$(env_or_default "$rw_var" PERF_FIO_RW randread)"
                bs="$(env_or_default "$bs_var" PERF_FIO_BS 4m)"
                size="$(env_or_default "$size_var" PERF_FIO_SIZE 512m)"
                numjobs="$(env_or_default "$numjobs_var" PERF_FIO_NUMJOBS 4)"
                ioengine="$(env_or_default "$ioengine_var" PERF_FIO_IOENGINE io_uring)"
                iodepth="$(env_or_default "$iodepth_var" PERF_FIO_IODEPTH 1)"
                direct="${direct_override:-$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)}"
                runtime="$(env_or_default "$runtime_var" PERF_FIO_RUNTIME 60)"
                use_time_based=true
                use_end_fsync=false
                use_refill_buffers=false
                needs_prefill=true
                ;;
            randwrite)
                name="$(env_or_default "$name_var" PERF_FIO_NAME brewfs-randwrite)"
                rw="$(env_or_default "$rw_var" PERF_FIO_RW randwrite)"
                bs="$(env_or_default "$bs_var" PERF_FIO_BS 4m)"
                size="$(env_or_default "$size_var" PERF_FIO_SIZE 512m)"
                numjobs="$(env_or_default "$numjobs_var" PERF_FIO_NUMJOBS 4)"
                ioengine="$(env_or_default "$ioengine_var" PERF_FIO_IOENGINE io_uring)"
                iodepth="$(env_or_default "$iodepth_var" PERF_FIO_IODEPTH 1)"
                direct="${direct_override:-$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)}"
                runtime="$(env_or_default "$runtime_var" PERF_FIO_RUNTIME 60)"
                use_time_based=true
                use_end_fsync=false
                use_refill_buffers=false
                ;;
            randrw)
                name="$(env_or_default "$name_var" PERF_FIO_NAME brewfs-randrw)"
                rw="$(env_or_default "$rw_var" PERF_FIO_RW randrw)"
                rwmixread="$(env_or_default "$rwmixread_var" PERF_FIO_RWMIXREAD 70)"
                bs="$(env_or_default "$bs_var" PERF_FIO_BS 4m)"
                size="$(env_or_default "$size_var" PERF_FIO_SIZE 512m)"
                numjobs="$(env_or_default "$numjobs_var" PERF_FIO_NUMJOBS 4)"
                ioengine="$(env_or_default "$ioengine_var" PERF_FIO_IOENGINE io_uring)"
                iodepth="$(env_or_default "$iodepth_var" PERF_FIO_IODEPTH 1)"
                direct="${direct_override:-$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)}"
                runtime="$(env_or_default "$runtime_var" PERF_FIO_RUNTIME 60)"
                use_time_based=true
                use_end_fsync=false
                use_refill_buffers=false
                needs_prefill=true
                ;;
            bigwrite)
                name="$(env_or_default "$name_var" PERF_FIO_NAME brewfs-bigwrite)"
                rw="$(env_or_default "$rw_var" PERF_FIO_RW write)"
                bs="$(env_or_default "$bs_var" PERF_FIO_BS 4m)"
                size="$(env_or_default "$size_var" PERF_FIO_SIZE 128m)"
                numjobs="$(env_or_default "$numjobs_var" PERF_FIO_NUMJOBS 8)"
                ioengine="$(env_or_default "$ioengine_var" PERF_FIO_IOENGINE io_uring)"
                iodepth="$(env_or_default "$iodepth_var" PERF_FIO_IODEPTH 1)"
                direct="${direct_override:-$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)}"
                runtime="0"
                use_time_based=false
                use_end_fsync=true
                use_refill_buffers=true
                needs_prefill=false
                ;;
            bigread)
                name="$(env_or_default "$name_var" PERF_FIO_NAME brewfs-bigread)"
                rw="$(env_or_default "$rw_var" PERF_FIO_RW read)"
                bs="$(env_or_default "$bs_var" PERF_FIO_BS 4m)"
                size="$(env_or_default "$size_var" PERF_FIO_SIZE 128m)"
                numjobs="$(env_or_default "$numjobs_var" PERF_FIO_NUMJOBS 8)"
                ioengine="$(env_or_default "$ioengine_var" PERF_FIO_IOENGINE io_uring)"
                iodepth="$(env_or_default "$iodepth_var" PERF_FIO_IODEPTH 1)"
                direct="${direct_override:-$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)}"
                runtime="0"
                use_time_based=false
                use_refill_buffers=true
                needs_prefill=true
                ;;
            *)
                err "未知的 fio profile: $mode"
                return 1
                ;;
        esac

        args=(
            --name="$name"
            --directory="$work_dir"
            --rw="$rw"
            --bs="$bs"
            --size="$size"
            --numjobs="$numjobs"
            --ioengine="$ioengine"
            --iodepth="$iodepth"
            --direct="$direct"
        )

        if [[ "${use_time_based:-true}" == true ]]; then
            args+=(--runtime="$runtime" --time_based)
        fi
        if [[ "${use_end_fsync:-false}" == true ]]; then
            args+=(--end_fsync=1)
        fi
        if [[ "${use_refill_buffers:-false}" == true ]]; then
            args+=(--refill_buffers)
        fi

        args+=(--group_reporting --eta=never)

        if [[ -n "${rwmixread:-}" ]]; then
            args+=(--rwmixread="$rwmixread")
        fi
    fi

    if [[ "$needs_prefill" == true ]]; then
        stats_snapshot_before_tool "${tool}-prefill"
        prepare_fio_dataset "$tool" "$work_dir" "$name" "$size" "$direct" "$numjobs" "$bs" "$ioengine" "$iodepth" || return $?
        stats_snapshot_after_tool "${tool}-prefill"
        if truthy_env "${PERF_FIO_COLD_READ:-false}" || truthy_env "${PERF_FIO_PREFILL_DRAIN:-false}"; then
            wait_for_fio_prefill_drain "$tool" || return $?
        fi
        if truthy_env "${PERF_FIO_COLD_READ:-false}" || truthy_env "${PERF_FIO_PREFILL_REMOUNT:-false}"; then
            remount_brewfs_for_fio_profile "$tool" || return $?
        fi
    fi

    # Collect per-second latency logs for time-series analysis
    local lat_log_prefix="$artifact_dir/results/${tool}_lat"
    args+=(--output-format=json --output="$json_path")
    args+=(--write_lat_log="$lat_log_prefix" --log_avg_msec=1000)
    run_logged_tool "$tool" fio "${args[@]}"
    append_fio_log_summary "$json_path" "$artifact_dir/tools/${tool}.log" "$tool"
    wait_for_fio_post_write_drain "$tool"
}

run_object_put_bench() {
    if [[ "$data_backend" != "s3" ]]; then
        err "object-put-bench requires BREWFS_DATA_BACKEND=s3"
        return 1
    fi

    local bucket="${BREWFS_S3_BUCKET:-brewfs-data}"
    local endpoint="${BREWFS_S3_ENDPOINT:-http://rustfs:9000}"
    local region="${BREWFS_S3_REGION:-us-east-1}"
    local force_path="${BREWFS_S3_FORCE_PATH_STYLE:-true}"
    local part_size="${BREWFS_S3_PART_SIZE:-16777216}"
    local s3_max_conc="${BREWFS_S3_MAX_CONCURRENCY:-8}"
    local checksum_disabled="${BREWFS_S3_DISABLE_PAYLOAD_CHECKSUM:-true}"
    local object_size="${PERF_OBJECT_PUT_OBJECT_SIZE:-${BREWFS_BLOCK_SIZE:-4194304}}"
    local workers="${PERF_OBJECT_PUT_WORKERS:-${BREWFS_UPLOAD_CONCURRENCY:-32}}"
    local duration="${PERF_OBJECT_PUT_DURATION_SECS:-60}"
    local objects="${PERF_OBJECT_PUT_OBJECTS:-0}"
    local prefix="${PERF_OBJECT_PUT_PREFIX:-bench/direct-put}"

    export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-rustfsadmin}"
    export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-rustfsadmin}"
    export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$region}"
    export AWS_EC2_METADATA_DISABLED="${AWS_EC2_METADATA_DISABLED:-true}"

    run_logged_tool object-put-bench /usr/local/bin/brewfs object-put-bench \
        --s3-bucket "$bucket" \
        --s3-endpoint "$endpoint" \
        --s3-region "$region" \
        --s3-part-size "$part_size" \
        --s3-max-concurrency "$s3_max_conc" \
        --s3-force-path-style "$force_path" \
        --s3-disable-payload-checksum "$checksum_disabled" \
        --object-size "$object_size" \
        --workers "$workers" \
        --duration-secs "$duration" \
        --objects "$objects" \
        --prefix "$prefix"
}

generate_perf_report() {
    python3 - "$artifact_dir" "$meta_backend" <<'PY'
import csv
import datetime as dt
import json
import pathlib
import sys

artifact_dir = pathlib.Path(sys.argv[1])
meta_backend = sys.argv[2]
summary_path = artifact_dir / "perf-summary.tsv"
report_path = artifact_dir / "report.md"
fio_json_paths = sorted((artifact_dir / "results").glob("fio*.json"))

rows = []
if summary_path.exists():
    with summary_path.open(newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
summary_by_tool = {row.get("tool", ""): row for row in rows}

lines = [
    "# BrewFS Perf Report",
    "",
    f"Meta backend: {meta_backend}",
    "",
    "## Summary",
    "",
    "| Tool | Status | Seconds | Log |",
    "| --- | --- | ---: | --- |",
]

for row in rows:
    log = pathlib.Path(row.get("log", "")).name
    lines.append(
        f"| {row.get('tool', '')} | {row.get('status', '')} | "
        f"{row.get('seconds', '')} | tools/{log} |"
    )

post_write_drain_path = artifact_dir / "post-write-drain.tsv"
if post_write_drain_path.exists():
    with post_write_drain_path.open(newline="") as f:
        drain_rows = [
            row
            for row in csv.DictReader(f, delimiter="\t")
            if row.get("tool")
        ]
    if drain_rows:
        lines.extend([
            "",
            "## Post-Write Drain",
            "",
            "| Tool | Drain seconds | Pending bytes | Dirty bytes | Buffer dirty bytes |",
            "| --- | ---: | ---: | ---: | ---: |",
        ])
        for row in drain_rows:
            lines.append(
                f"| {row.get('tool', '')} | {row.get('post_fio_drain_s', '')} | "
                f"{row.get('pending_bytes', '')} | {row.get('dirty_bytes', '')} | "
                f"{row.get('buffer_dirty_bytes', '')} |"
            )

if fio_json_paths:
    try:
        def num(value, default=0):
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        def fmt_bytes(value):
            value = num(value)
            units = ["B", "KiB", "MiB", "GiB", "TiB"]
            for unit in units:
                if abs(value) < 1024 or unit == units[-1]:
                    return f"{value:.2f} {unit}"
                value /= 1024
            return f"{value:.2f} TiB"

        def fmt_rate(value):
            return f"{fmt_bytes(value)}/s"

        def fmt_iops(value):
            return f"{num(value):,.2f}"

        def fmt_ms_from_ns(value):
            return f"{num(value) / 1_000_000:.3f} ms"

        def fmt_seconds_from_ms(value):
            value = num(value)
            if value <= 0:
                return "-"
            return f"{value / 1000.0:.3f} s"

        def fmt_delta_ms(value):
            value = num(value)
            sign = "+" if value >= 0 else "-"
            return f"{sign}{abs(value) / 1000.0:.3f} s"

        def fmt_ratio(value):
            value = num(value)
            if value <= 0:
                return "-"
            return f"{value:.2f}x"

        def latency_percentile(op, pct):
            percentiles = op.get("clat_ns", {}).get("percentile", {})
            return percentiles.get(f"{pct:.6f}") or percentiles.get(str(pct))

        def op_totals(op_name):
            ops = [job.get(op_name, {}) for job in jobs]
            io_bytes = sum(num(op.get("io_bytes")) for op in ops)
            bw_bytes = sum(num(op.get("bw_bytes")) for op in ops)
            iops = sum(num(op.get("iops")) for op in ops)
            total_ios = sum(num(op.get("total_ios")) for op in ops)
            runtimes = [num(op.get("runtime")) for op in ops if num(op.get("runtime")) > 0]
            runtime_ms = max(runtimes) if runtimes else 0
            means = [
                (num(op.get("clat_ns", {}).get("mean")), num(op.get("clat_ns", {}).get("N")))
                for op in ops
                if num(op.get("clat_ns", {}).get("N")) > 0
            ]
            total_n = sum(n for _, n in means)
            mean_ns = sum(mean * n for mean, n in means) / total_n if total_n else 0
            p95 = max((num(latency_percentile(op, 95)) for op in ops), default=0)
            p99 = max((num(latency_percentile(op, 99)) for op in ops), default=0)
            return {
                "io_bytes": io_bytes,
                "bw_bytes": bw_bytes,
                "iops": iops,
                "total_ios": total_ios,
                "runtime_ms": runtime_ms,
                "mean_ns": mean_ns,
                "p95_ns": p95,
                "p99_ns": p99,
            }

        def first_job_options():
            for job in jobs:
                options = job.get("job options", {})
                if options:
                    return options
            return {}

        def fio_runtime_accounting(data, tool_name, read, write):
            jobs = data.get("jobs", [])
            raw_job_runtime_ms = max((num(job.get("job_runtime")) for job in jobs), default=0)
            active_runtime_ms = max(read["runtime_ms"], write["runtime_ms"])
            wall_seconds = num(summary_by_tool.get(tool_name, {}).get("seconds"))
            wall_ms = wall_seconds * 1000.0 if wall_seconds > 0 else 0
            wall_vs_job_ms = wall_ms - raw_job_runtime_ms if raw_job_runtime_ms > 0 and wall_ms > 0 else 0
            wall_vs_active_ms = wall_ms - active_runtime_ms if active_runtime_ms > 0 and wall_ms > 0 else 0
            wall_job_ratio = wall_ms / raw_job_runtime_ms if raw_job_runtime_ms > 0 and wall_ms > 0 else 0
            wall_active_ratio = wall_ms / active_runtime_ms if active_runtime_ms > 0 and wall_ms > 0 else 0
            tail = "-"
            if wall_vs_active_ms > 5000 or wall_active_ratio > 1.15:
                tail = "close/flush tail"
            elif raw_job_runtime_ms > wall_ms > 0:
                tail = "fio job_runtime aggregates jobs"
            return {
                "raw_job_runtime_ms": raw_job_runtime_ms,
                "active_runtime_ms": active_runtime_ms,
                "wall_seconds": wall_seconds,
                "wall_vs_job_ms": wall_vs_job_ms,
                "wall_vs_active_ms": wall_vs_active_ms,
                "wall_job_ratio": wall_job_ratio,
                "wall_active_ratio": wall_active_ratio,
                "tail": tail,
            }

        lines.extend([
            "",
            "## Fio",
            "",
            "| Tool | Workload | Direct | BS | Jobs | Read BW | Read IOPS | Write BW | Write IOPS | Read P99 | Write P99 | Raw |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |---: | ---: | ---: |",
        ])

        runtime_rows = []
        for fio_json_path in fio_json_paths:
            data = json.loads(fio_json_path.read_text())
            jobs = data.get("jobs", [])
            if not jobs:
                continue
            options = first_job_options()
            read = op_totals("read")
            write = op_totals("write")
            tool_name = fio_json_path.stem
            runtime_rows.append((tool_name, options, fio_runtime_accounting(data, tool_name, read, write)))
            lines.append(
                f"| {tool_name} | {options.get('rw', 'unknown')} | {options.get('direct', 'unknown')} | "
                f"{options.get('bs', 'unknown')} | "
                f"{options.get('numjobs', 'unknown')} | {fmt_rate(read['bw_bytes'])} | "
                f"{fmt_iops(read['iops'])} | {fmt_rate(write['bw_bytes'])} | "
                f"{fmt_iops(write['iops'])} | {fmt_ms_from_ns(read['p99_ns'])} | "
                f"{fmt_ms_from_ns(write['p99_ns'])} | results/{fio_json_path.name} |"
            )

        if runtime_rows:
            lines.extend([
                "",
                "## Fio Runtime Accounting",
                "",
                "Use `active_io_runtime` for close/flush tail detection; fio `job_runtime_ms` can aggregate multiple jobs under group reporting.",
                "",
                "| Tool | Direct | Script wall | fio job_runtime | wall-job_runtime | wall/job_runtime | active_io_runtime | wall-active_io | wall/active_io | Tail marker |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
            ])
            for tool_name, options, runtime in runtime_rows:
                lines.append(
                    f"| {tool_name} | {options.get('direct', 'unknown')} | "
                    f"{runtime['wall_seconds']:.0f} s | {fmt_seconds_from_ms(runtime['raw_job_runtime_ms'])} | "
                    f"{fmt_delta_ms(runtime['wall_vs_job_ms'])} | {fmt_ratio(runtime['wall_job_ratio'])} | "
                    f"{fmt_seconds_from_ms(runtime['active_runtime_ms'])} | {fmt_delta_ms(runtime['wall_vs_active_ms'])} | "
                    f"{fmt_ratio(runtime['wall_active_ratio'])} | {runtime['tail']} |"
                )
    except Exception as exc:
        lines.extend(["", "## Fio", "", f"Failed to parse fio JSON: {exc}"])

# --- Detailed Latency Percentiles ---
if fio_json_paths:
    try:
        pct_keys = ["1.000000", "5.000000", "25.000000", "50.000000",
                    "75.000000", "90.000000", "95.000000", "99.000000", "99.900000"]
        pct_labels = ["p1", "p5", "p25", "p50", "p75", "p90", "p95", "p99", "p99.9"]

        lines.extend([
            "",
            "## Latency Percentiles",
            "",
            "### Read",
            "",
            "| Workload | " + " | ".join(pct_labels) + " |",
            "| --- |" + " ---: |" * len(pct_labels),
        ])
        for fio_json_path in fio_json_paths:
            data = json.loads(fio_json_path.read_text())
            jobs = data.get("jobs", [])
            if not jobs:
                continue
            read_op = jobs[0].get("read", {})
            percs = read_op.get("clat_ns", {}).get("percentile", {})
            if not percs or num(read_op.get("bw_bytes")) == 0:
                continue
            cols = [fio_json_path.stem]
            for k in pct_keys:
                cols.append(fmt_ms_from_ns(percs.get(k, 0)))
            lines.append("| " + " | ".join(cols) + " |")

        lines.extend([
            "",
            "### Write",
            "",
            "| Workload | " + " | ".join(pct_labels) + " |",
            "| --- |" + " ---: |" * len(pct_labels),
        ])
        for fio_json_path in fio_json_paths:
            data = json.loads(fio_json_path.read_text())
            jobs = data.get("jobs", [])
            if not jobs:
                continue
            write_op = jobs[0].get("write", {})
            percs = write_op.get("clat_ns", {}).get("percentile", {})
            if not percs or num(write_op.get("bw_bytes")) == 0:
                continue
            cols = [fio_json_path.stem]
            for k in pct_keys:
                cols.append(fmt_ms_from_ns(percs.get(k, 0)))
            lines.append("| " + " | ".join(cols) + " |")
    except Exception:
        pass

# --- Direct Object PUT Benchmark ---
object_put_logs = sorted((artifact_dir / "tools").glob("object-put-bench*.log"))
object_put_rows = []
for path in object_put_logs:
    try:
        for raw in path.read_text(errors="replace").splitlines():
            if not raw.startswith("object_put_bench_summary "):
                continue
            fields = {}
            for item in raw.split()[1:]:
                if "=" in item:
                    key, value = item.split("=", 1)
                    fields[key] = value
            fields["log"] = str(path.relative_to(artifact_dir))
            object_put_rows.append(fields)
            break
    except Exception:
        pass

if object_put_rows:
    lines.extend([
        "",
        "## Direct Object PUT",
        "",
        "| Tool | Objects | Bytes | Seconds | MiB/s | Workers | Object size | Avg | P50 | P90 | P95 | P99 | Max | Log |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ])
    for row in object_put_rows:
        lines.append(
            f"| object-put-bench | {row.get('objects', '')} | {row.get('bytes', '')} | "
            f"{row.get('seconds', '')} | {row.get('throughput_mib_s', '')} | "
            f"{row.get('workers', '')} | {row.get('object_size', '')} | "
            f"{row.get('avg_ms', '')} ms | {row.get('p50_ms', '')} ms | "
            f"{row.get('p90_ms', '')} ms | {row.get('p95_ms', '')} ms | "
            f"{row.get('p99_ms', '')} ms | {row.get('max_ms', '')} ms | {row.get('log', '')} |"
        )

# --- Metadata Performance ---
metadata_rows = []
for meta_log in sorted((artifact_dir / "tools").glob("*.log")):
    try:
        for mline in meta_log.read_text(errors="replace").splitlines():
            if mline.startswith("metadata_summary "):
                row = {"log": str(meta_log.relative_to(artifact_dir))}
                for item in mline.split()[1:]:
                    if "=" in item:
                        key, value = item.split("=", 1)
                        row[key] = value
                metadata_rows.append(row)
            elif "ops/sec=" in mline and "usec/op" in mline:
                op = mline.split(":")[0].strip()
                ops_sec = mline.split("ops/sec=")[1].split(",")[0]
                usec_op = mline.split("usec/op")[1].strip().lstrip("= ")
                metadata_rows.append(
                    {
                        "tool": meta_log.stem,
                        "op": op,
                        "ops_per_sec": ops_sec,
                        "usec_per_op": usec_op,
                        "log": str(meta_log.relative_to(artifact_dir)),
                    }
                )
    except Exception:
        pass

if metadata_rows:
    lines.extend([
        "",
        "## Metadata Performance",
        "",
        "| Tool | Operation | Ops | Errors | Ops/sec | Latency (µs/op) | Log |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- |",
    ])
    for row in metadata_rows:
        lines.append(
            f"| {row.get('tool', '')} | {row.get('op', '')} | "
            f"{row.get('ops', '')} | {row.get('errors', '')} | "
            f"{float(row.get('ops_per_sec', 0.0)):.1f} | "
            f"{float(row.get('usec_per_op', 0.0)):.0f} | {row.get('log', '')} |"
        )

# --- Diagnostics ---
diag_dir = artifact_dir / "diagnostics"
redis_diag_paths = sorted(diag_dir.glob("redis-*-after.txt")) if diag_dir.exists() else []
if redis_diag_paths:
    lines.extend([
        "",
        "## Redis Diagnostics",
        "",
        "| Tool | Top commandstats | Details |",
        "| --- | --- | --- |",
    ])

    def parse_commandstats(path):
        commands = []
        for raw in path.read_text(errors="replace").splitlines():
            if not raw.startswith("cmdstat_") or ":" not in raw:
                continue
            name, payload = raw.split(":", 1)
            fields = {}
            for item in payload.split(","):
                if "=" in item:
                    key, value = item.split("=", 1)
                    fields[key] = value
            try:
                calls = int(float(fields.get("calls", "0")))
                usec_per_call = float(fields.get("usec_per_call", "0"))
            except ValueError:
                continue
            if calls > 0:
                commands.append((name.removeprefix("cmdstat_"), calls, usec_per_call))
        return sorted(commands, key=lambda item: item[1] * item[2], reverse=True)[:5]

    for path in redis_diag_paths:
        tool = path.name.removeprefix("redis-").removesuffix("-after.txt")
        top = parse_commandstats(path)
        if top:
            summary = "<br>".join(
                f"{cmd}: calls={calls}, usec/call={usec_per_call:.2f}"
                for cmd, calls, usec_per_call in top
            )
        else:
            summary = "n/a"
        rel = path.relative_to(artifact_dir)
        lines.append(f"| {tool} | {summary} | {rel} |")

brewfs_stats_paths = sorted(diag_dir.glob("stats-*-after.txt")) if diag_dir.exists() else []
if brewfs_stats_paths:
    sample_summaries = {}
    sample_path = artifact_dir / "writeback-samples.tsv"
    if sample_path.exists():
        try:
            import csv

            def as_float(value):
                try:
                    return float(value)
                except Exception:
                    return 0.0

            with sample_path.open(newline="") as fh:
                for row in csv.DictReader(fh, delimiter="\t"):
                    tool = row.get("tool", "")
                    if not tool:
                        continue
                    summary = sample_summaries.setdefault(
                        tool,
                        {
                            "count": 0,
                            "max_buffer_dirty": 0.0,
                            "max_live_dirty": 0.0,
                            "max_live_slices": 0.0,
                            "max_recent_pending": 0.0,
                            "max_recent_uploaded": 0.0,
                            "max_stage_inflight": 0.0,
                            "max_remote_inflight": 0.0,
                            "last_buffer_dirty": 0.0,
                            "last_live_dirty": 0.0,
                            "last_recent_pending": 0.0,
                            "last_recent_uploaded": 0.0,
                        },
                    )
                    summary["count"] += 1
                    buffer_dirty = as_float(row.get("buffer_dirty_bytes"))
                    live_dirty = as_float(row.get("live_dirty_bytes"))
                    live_slices = as_float(row.get("live_slices"))
                    recent_pending = as_float(row.get("recent_pending_upload_bytes"))
                    recent_uploaded = as_float(row.get("recent_uploaded_bytes"))
                    stage_inflight = as_float(row.get("stage_inflight_bytes"))
                    remote_inflight = as_float(row.get("remote_upload_inflight_bytes"))
                    summary["max_buffer_dirty"] = max(summary["max_buffer_dirty"], buffer_dirty)
                    summary["max_live_dirty"] = max(summary["max_live_dirty"], live_dirty)
                    summary["max_live_slices"] = max(summary["max_live_slices"], live_slices)
                    summary["max_recent_pending"] = max(summary["max_recent_pending"], recent_pending)
                    summary["max_recent_uploaded"] = max(summary["max_recent_uploaded"], recent_uploaded)
                    summary["max_stage_inflight"] = max(summary["max_stage_inflight"], stage_inflight)
                    summary["max_remote_inflight"] = max(summary["max_remote_inflight"], remote_inflight)
                    summary["last_buffer_dirty"] = buffer_dirty
                    summary["last_live_dirty"] = live_dirty
                    summary["last_recent_pending"] = recent_pending
                    summary["last_recent_uploaded"] = recent_uploaded
        except Exception:
            sample_summaries = {}

    lines.extend([
        "",
        "## BrewFS Stats",
        "",
        "Counters are per-tool deltas when matching before/after snapshots exist; dirty, live, and inflight values are after-tool gauges.",
        "",
        "| Tool | Cache hit | FUSE read | FUSE write | Dirty | Live dirty | Recent pending | Recent uploaded | Read buffer | S3 ops | S3 avg latency | Details |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ])

    def parse_brewfs_stats(path):
        metrics = {}
        for raw in path.read_text(errors="replace").splitlines():
            raw = raw.strip()
            if not raw.startswith("brewfs_"):
                continue
            parts = raw.split()
            if len(parts) < 2:
                continue
            try:
                metrics[parts[0]] = float(parts[1])
            except ValueError:
                continue
        return metrics

    def fmt_mib(value):
        return f"{value / 1048576.0:.1f} MiB"

    def fmt_avg_mib(bytes_value, count):
        return fmt_mib(bytes_value / count) if count else "n/a"

    for path in brewfs_stats_paths:
        tool = path.name.removeprefix("stats-").removesuffix("-after.txt")
        metrics = parse_brewfs_stats(path)
        before_path = path.with_name(path.name.replace("-after.txt", "-before.txt"))
        before_metrics = parse_brewfs_stats(before_path) if before_path.exists() else {}

        def value(name, default=0.0):
            return metrics.get(name, default)

        def delta(name, default=0.0):
            current = metrics.get(name, default)
            if not before_metrics:
                return current
            previous = before_metrics.get(name, default)
            return max(0.0, current - previous)

        def delta_avg_us(lat_total_name, ops_name, avg_name):
            ops = delta(ops_name)
            if ops:
                return delta(lat_total_name) / ops
            return value(avg_name, 0.0)

        hits = delta("brewfs_cache_hits_total")
        misses = delta("brewfs_cache_misses_total")
        requests = delta("brewfs_cache_requests_total", hits + misses)
        if before_metrics and requests == 0:
            requests = hits + misses
        hit_ratio = metrics.get(
            "brewfs_cache_hit_ratio",
            hits / requests if requests else 0.0,
        )
        if before_metrics:
            hit_ratio = hits / requests if requests else 0.0
        fuse_read = delta("brewfs_fuse_read_bytes_total")
        fuse_write = delta("brewfs_fuse_write_bytes_total")
        dirty = value(
            "brewfs_writeback_dirty_bytes",
            value("brewfs_buffer_dirty_bytes", 0.0),
        )
        live_dirty = value("brewfs_writeback_live_dirty_bytes", 0.0)
        recent_pending = value("brewfs_writeback_recent_pending_upload_bytes", 0.0)
        recent_uploaded = value("brewfs_writeback_recent_uploaded_bytes", 0.0)
        read_buffer = value(
            "brewfs_reader_buffer_bytes",
            value("brewfs_buffer_read_bytes", 0.0),
        )
        s3_get = delta("brewfs_s3_get_ops_total")
        s3_put = delta("brewfs_s3_put_ops_total")
        s3_get_avg_ms = delta_avg_us(
            "brewfs_s3_get_lat_us_total",
            "brewfs_s3_get_ops_total",
            "brewfs_s3_get_avg_lat_us",
        ) / 1000.0
        s3_put_avg_ms = delta_avg_us(
            "brewfs_s3_put_lat_us_total",
            "brewfs_s3_put_ops_total",
            "brewfs_s3_put_avg_lat_us",
        ) / 1000.0
        range_gets = delta("brewfs_read_range_gets_total")
        full_gets = delta("brewfs_read_full_gets_total")
        bg_prefetch = delta("brewfs_read_background_prefetch_total")
        stage_ops = delta("brewfs_writeback_stage_ops_total")
        stage_bytes = delta("brewfs_writeback_stage_bytes_total")
        stage_ms = delta("brewfs_writeback_stage_lat_us_total") / 1000.0
        stage_s = delta("brewfs_writeback_stage_lat_us_total") / 1_000_000.0
        commit_wait_s = delta("brewfs_writeback_commit_wait_upload_us_total") / 1_000_000.0
        flush_wait_ops = delta("brewfs_writeback_flush_wait_ops_total")
        flush_wait_s = delta("brewfs_writeback_flush_wait_us_total") / 1_000_000.0
        flush_wait_slices = delta("brewfs_writeback_flush_wait_slices_total")
        stage_failures = delta("brewfs_writeback_stage_failures_total")
        commit_before_stage = delta(
            "brewfs_writeback_commit_before_stage_ops_total",
        )
        remote_inflight = metrics.get("brewfs_writeback_remote_upload_inflight_bytes", 0.0)
        buffer_soft_ops = metrics.get("brewfs_writeback_buffer_soft_sleep_ops", 0.0)
        buffer_soft_ms = metrics.get("brewfs_writeback_buffer_soft_sleep_us", 0.0) / 1000.0
        buffer_moderate_ops = metrics.get("brewfs_writeback_buffer_moderate_sleep_ops", 0.0)
        buffer_moderate_ms = (
            metrics.get("brewfs_writeback_buffer_moderate_sleep_us", 0.0) / 1000.0
        )
        buffer_hard_ops = metrics.get("brewfs_writeback_buffer_hard_sleep_ops", 0.0)
        buffer_hard_ms = metrics.get("brewfs_writeback_buffer_hard_sleep_us", 0.0) / 1000.0
        live_slices = metrics.get("brewfs_writeback_live_slices", 0.0)
        live_normal_only_slices = metrics.get("brewfs_writeback_live_normal_only_slices", 0.0)
        live_cached_only_slices = metrics.get("brewfs_writeback_live_cached_only_slices", 0.0)
        live_mixed_origin_slices = metrics.get("brewfs_writeback_live_mixed_origin_slices", 0.0)
        live_unknown_origin_slices = metrics.get("brewfs_writeback_live_unknown_origin_slices", 0.0)
        slice_create = metrics.get("brewfs_writeback_slice_create_ops_total", 0.0)
        slice_reuse = metrics.get("brewfs_writeback_slice_reuse_ops_total", 0.0)
        reject_older = metrics.get("brewfs_writeback_slice_reject_older_unique_ops_total", 0.0)
        reject_prefix = metrics.get(
            "brewfs_writeback_slice_reject_dispatched_prefix_ops_total",
            0.0,
        )
        batch_ops = delta("brewfs_writeback_upload_batch_ops_total")
        batch_bytes = delta("brewfs_writeback_upload_batch_bytes_total")
        batch_blocks = delta("brewfs_writeback_upload_batch_blocks_total")
        partial_tail = delta("brewfs_writeback_upload_partial_tail_ops_total")
        partial_tail_size = delta("brewfs_writeback_upload_partial_tail_size_ops_total")
        partial_tail_max = delta(
            "brewfs_writeback_upload_partial_tail_max_unflushed_ops_total",
        )
        partial_tail_flush = delta(
            "brewfs_writeback_upload_partial_tail_explicit_flush_ops_total",
        )
        partial_tail_auto = delta("brewfs_writeback_upload_partial_tail_auto_ops_total")
        partial_tail_normal = delta(
            "brewfs_writeback_upload_partial_tail_normal_only_ops_total",
        )
        partial_tail_cached = delta(
            "brewfs_writeback_upload_partial_tail_cached_only_ops_total",
        )
        partial_tail_mixed = delta(
            "brewfs_writeback_upload_partial_tail_mixed_origin_ops_total",
        )
        partial_tail_unknown_origin = delta(
            "brewfs_writeback_upload_partial_tail_unknown_origin_ops_total",
        )
        partial_tail_auto_age = delta(
            "brewfs_writeback_upload_partial_tail_auto_age_ops_total",
        )
        partial_tail_auto_idle = delta(
            "brewfs_writeback_upload_partial_tail_auto_idle_ops_total",
        )
        partial_tail_auto_pressure = delta(
            "brewfs_writeback_upload_partial_tail_auto_pressure_ops_total",
        )
        partial_tail_auto_too_many = delta(
            "brewfs_writeback_upload_partial_tail_auto_too_many_ops_total",
        )
        partial_tail_auto_buffer_high = delta(
            "brewfs_writeback_upload_partial_tail_auto_buffer_high_ops_total",
        )
        partial_tail_auto_flush_duration = delta(
            "brewfs_writeback_upload_partial_tail_auto_flush_duration_ops_total",
        )
        partial_tail_auto_unknown = delta(
            "brewfs_writeback_upload_partial_tail_auto_unknown_ops_total",
        )
        partial_tail_auto_normal = delta(
            "brewfs_writeback_upload_partial_tail_auto_normal_only_ops_total",
        )
        partial_tail_auto_cached = delta(
            "brewfs_writeback_upload_partial_tail_auto_cached_only_ops_total",
        )
        partial_tail_auto_mixed = delta(
            "brewfs_writeback_upload_partial_tail_auto_mixed_origin_ops_total",
        )
        partial_tail_auto_unknown_origin = delta(
            "brewfs_writeback_upload_partial_tail_auto_unknown_origin_ops_total",
        )
        partial_tail_age = delta(
            "brewfs_writeback_upload_partial_tail_commit_age_ops_total",
        )
        sample = sample_summaries.get(tool, {})
        sample_detail = "samples=n/a"
        if sample:
            sample_detail = (
                f"samples={int(sample.get('count', 0))}, "
                f"max_buffer_dirty={fmt_mib(sample.get('max_buffer_dirty', 0.0))}, "
                f"max_live_dirty={fmt_mib(sample.get('max_live_dirty', 0.0))}, "
                f"max_live_slices={int(sample.get('max_live_slices', 0.0))}, "
                f"max_recent_pending={fmt_mib(sample.get('max_recent_pending', 0.0))}, "
                f"max_recent_uploaded={fmt_mib(sample.get('max_recent_uploaded', 0.0))}, "
                f"max_stage_inflight={fmt_mib(sample.get('max_stage_inflight', 0.0))}, "
                f"max_remote_inflight={fmt_mib(sample.get('max_remote_inflight', 0.0))}, "
                f"last_buffer_dirty={fmt_mib(sample.get('last_buffer_dirty', 0.0))}"
            )
        freeze_size = metrics.get("brewfs_writeback_freeze_size_ops_total", 0.0)
        freeze_flush = metrics.get("brewfs_writeback_freeze_explicit_flush_ops_total", 0.0)
        freeze_auto = metrics.get("brewfs_writeback_freeze_auto_ops_total", 0.0)
        freeze_max = metrics.get("brewfs_writeback_freeze_max_unflushed_ops_total", 0.0)
        freeze_age = metrics.get("brewfs_writeback_freeze_commit_age_ops_total", 0.0)
        avg_batch_blocks = batch_blocks / batch_ops if batch_ops else 0.0
        partial_tail_ratio = partial_tail / batch_ops if batch_ops else 0.0
        rel = path.relative_to(artifact_dir)
        lines.append(
            f"| {tool} | {hit_ratio * 100.0:.1f}% ({int(hits)}/{int(requests)}) | "
            f"{fmt_mib(fuse_read)} | {fmt_mib(fuse_write)} | {fmt_mib(dirty)} | "
            f"{fmt_mib(live_dirty)} | {fmt_mib(recent_pending)} | {fmt_mib(recent_uploaded)} | "
            f"{fmt_mib(read_buffer)} | GET={int(s3_get)}, PUT={int(s3_put)} | "
            f"GET={s3_get_avg_ms:.2f} ms, PUT={s3_put_avg_ms:.2f} ms | "
            f"{rel}; range={int(range_gets)}, full={int(full_gets)}, bg_prefetch={int(bg_prefetch)}, "
            f"stage={int(stage_ops)} ops/{fmt_mib(stage_bytes)}/{stage_ms:.1f} ms, "
            f"foreground=stage {stage_s:.2f}s/commit_wait {commit_wait_s:.2f}s, "
            f"flush_wait={int(flush_wait_ops)} ops/{flush_wait_s:.2f}s/{int(flush_wait_slices)} slices, "
            f"stage_fail={int(stage_failures)}, commit_before_stage={int(commit_before_stage)}, "
            f"remote_inflight={fmt_mib(remote_inflight)}, "
            f"buffer_sleep=soft {int(buffer_soft_ops)}/{buffer_soft_ms:.1f}ms "
            f"moderate {int(buffer_moderate_ops)}/{buffer_moderate_ms:.1f}ms "
            f"hard {int(buffer_hard_ops)}/{buffer_hard_ms:.1f}ms, "
            f"{sample_detail}, "
            f"slices=create {int(slice_create)}/reuse {int(slice_reuse)}/"
            f"reject_unique {int(reject_older)}/reject_prefix {int(reject_prefix)}, "
            f"live_slices={int(live_slices)} avg={fmt_avg_mib(live_dirty, live_slices)}, "
            f"origin=normal {int(live_normal_only_slices)}/cached {int(live_cached_only_slices)}/"
            f"mixed {int(live_mixed_origin_slices)}/unknown {int(live_unknown_origin_slices)}, "
            f"upload_batch={int(batch_ops)} avg={fmt_avg_mib(batch_bytes, batch_ops)} "
            f"blocks={avg_batch_blocks:.2f}/batch partial_tail={partial_tail_ratio:.2f} "
            f"(size {int(partial_tail_size)}/max {int(partial_tail_max)}/"
            f"flush {int(partial_tail_flush)}/auto {int(partial_tail_auto)}/"
            f"age {int(partial_tail_age)}), "
            f"partial_origin=normal {int(partial_tail_normal)}/cached {int(partial_tail_cached)}/"
            f"mixed {int(partial_tail_mixed)}/unknown {int(partial_tail_unknown_origin)}, "
            f"auto_detail=age {int(partial_tail_auto_age)}/idle {int(partial_tail_auto_idle)}/"
            f"pressure {int(partial_tail_auto_pressure)}/too_many {int(partial_tail_auto_too_many)}/"
            f"buffer_high {int(partial_tail_auto_buffer_high)}/"
            f"flush_duration {int(partial_tail_auto_flush_duration)}/"
            f"unknown {int(partial_tail_auto_unknown)}, "
            f"auto_origin=normal {int(partial_tail_auto_normal)}/"
            f"cached {int(partial_tail_auto_cached)}/mixed {int(partial_tail_auto_mixed)}/"
            f"unknown {int(partial_tail_auto_unknown_origin)}, "
            f"freeze=size {int(freeze_size)}/flush {int(freeze_flush)}/auto {int(freeze_auto)}/"
            f"max {int(freeze_max)}/age {int(freeze_age)} |"
        )

fuse_log = artifact_dir / "brewfs_fuse_ops.log"
if fuse_log.exists() and fuse_log.stat().st_size > 0:
    try:
        line_count = sum(1 for _ in fuse_log.open("rb"))
        lines.extend([
            "",
            "## FUSE Op Trace",
            "",
            f"- brewfs_fuse_ops.log: {line_count:,} lines",
        ])
    except Exception:
        pass

# --- Bottleneck Analysis ---
if fio_json_paths:
    try:
        findings = []
        for fio_json_path in fio_json_paths:
            data = json.loads(fio_json_path.read_text())
            jobs = data.get("jobs", [])
            if not jobs:
                continue
            job = jobs[0]
            options = job.get("job options", {})
            name = fio_json_path.stem
            numjobs = int(options.get("numjobs", 1))

            read_op = job.get("read", {})
            write_op = job.get("write", {})

            rpercs = read_op.get("clat_ns", {}).get("percentile", {})
            wpercs = write_op.get("clat_ns", {}).get("percentile", {})

            if rpercs and num(read_op.get("bw_bytes")) > 0:
                p50 = num(rpercs.get("50.000000")) / 1e6
                p99 = num(rpercs.get("99.000000")) / 1e6
                if p50 > 100:
                    findings.append(f"- **{name}**: Read p50={p50:.0f}ms — network RTT dominates. Consider local SSD cache or prefetch tuning.")
                elif p99 > p50 * 5 and p99 > 50:
                    findings.append(f"- **{name}**: Read tail latency p99/p50={p99/p50:.1f}x ({p50:.1f}ms→{p99:.0f}ms). Likely S3 retry or cache miss.")

            if wpercs and num(write_op.get("bw_bytes")) > 0:
                p50 = num(wpercs.get("50.000000")) / 1e6
                p99 = num(wpercs.get("99.000000")) / 1e6
                if p50 < 10 and p99 > 200:
                    findings.append(f"- **{name}**: Write stall p50={p50:.1f}ms p99={p99:.0f}ms — auto_flush/buffer-limit triggers S3 upload backpressure.")
                elif p99 > 500:
                    findings.append(f"- **{name}**: Write P99={p99:.0f}ms > 500ms — inspect writeback samples and tune upload queue depth, slice age, or write buffer.")

        if findings:
            lines.extend(["", "## Bottleneck Analysis", ""])
            lines.extend(findings)
    except Exception:
        pass

report_path.write_text("\n".join(lines) + "\n")
PY
}

run_perf_suite() {
    local -a tools=()
    local status=0
    local tool=""

    read -r -a tools <<<"$perf_tools"
    if [[ "${#tools[@]}" -eq 0 ]]; then
        err "PERF_TOOLS 不能为空"
        exit 1
    fi

    for tool in "${tools[@]}"; do
        case "$tool" in
            dirstress)
                run_dirstress || status=1
                ;;
            dirperf)
                run_dirperf || status=1
                ;;
            metaperf)
                run_metaperf || status=1
                ;;
            looptest)
                run_looptest || status=1
                ;;
            stress-ng)
                run_stress_ng || status=1
                ;;
            fio)
                run_fio_custom || status=1
                ;;
            fio-seqread)
                run_fio_profile "$tool" seqread || status=1
                ;;
            fio-seqwrite)
                run_fio_profile "$tool" seqwrite || status=1
                ;;
            fio-randread)
                run_fio_profile "$tool" randread || status=1
                ;;
            fio-randwrite)
                run_fio_profile "$tool" randwrite || status=1
                ;;
            fio-randrw)
                run_fio_profile "$tool" randrw || status=1
                ;;
            fio-bigwrite)
                run_fio_profile "$tool" bigwrite || status=1
                ;;
            fio-bigread)
                run_fio_profile "$tool" bigread || status=1
                ;;
            object-put-bench)
                run_object_put_bench || status=1
                ;;
            *)
                err "不支持的 PERF_TOOLS 项: $tool"
                status=1
                ;;
        esac
    done

    return "$status"
}

main() {
    if [[ -z "$artifact_dir" ]]; then
        local ts
        ts="$(date +%s)-$RANDOM"
        artifact_dir="${artifact_root%/}/perf-run-${ts}"
    fi

    mkdir -p "$artifact_dir"
    chmod a+rwx "$artifact_dir" >/dev/null 2>&1 || true
    log_file="$artifact_dir/brewfs.log"
    export BREWFS_LOG_FILE="$log_file"

    trap on_exit EXIT INT TERM
    raise_nofile_limit

    info "写入 BrewFS 配置: $config_path"
    write_config

    info "准备产物目录: $artifact_dir"
    prepare_artifacts

    info "安装 mount helper: /usr/sbin/mount.fuse.brewfs"
    install_mount_helper

    mount_brewfs

    # Pre-flight sanity check: verify the filesystem can create, write, and read files.
    info "执行挂载点预检: $mount_dir"
    local preflight_dir="$mount_dir/.perf-preflight"
    local preflight_file="$preflight_dir/test.bin"
    rm -rf "$preflight_dir"
    mkdir -p "$preflight_dir"
    if ! echo "brewfs-preflight-$(date +%s)" > "$preflight_file"; then
        err "预检失败: 无法写入 $preflight_file"
        exit 1
    fi
    local preflight_read
    preflight_read=$(cat "$preflight_file" 2>/dev/null)
    if [[ -z "$preflight_read" ]]; then
        err "预检失败: 无法读取 $preflight_file"
        exit 1
    fi
    rm -rf "$preflight_dir"
    ok "预检通过: 写入/读取正常"

    info "开始性能测试: tools=$perf_tools"
    set +e
    run_perf_suite
    status=$?
    set -e

    # Post-test filesystem statistics
    info "测试完成后文件系统统计:"
    if command -v df >/dev/null 2>&1; then
        df -h "$mount_dir" 2>/dev/null | tail -1 | while read -r fs size used avail pct mnt; do
            info "  磁盘使用: $used / $size ($pct)"
        done
    fi
    if [[ -d "$mount_dir" ]]; then
        local total_files
        total_files=$(find "$mount_dir" -type f 2>/dev/null | wc -l)
        info "  残留文件数: $total_files"
    fi

    generate_perf_report || true

    if [[ "$status" -eq 0 ]]; then
        ok "性能测试全部完成"
    else
        err "性能测试存在失败项 (exit=$status)"
    fi

    return "$status"
}

main "$@"
