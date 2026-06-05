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
            || -n "${BREWFS_PREFETCH_ENABLED:-}" \
            || -n "${BREWFS_PREFETCH_MAX_BYTES:-}" \
            || -n "${BREWFS_PREFETCH_CONCURRENCY:-}" \
            || -n "${BREWFS_MEMORY_BUDGET_BYTES:-}" \
            || -n "${BREWFS_COMPRESSION:-}" \
            || -n "${BREWFS_UPLOAD_LIMIT_MIBPS:-}" \
            || -n "${BREWFS_DOWNLOAD_LIMIT_MIBPS:-}" \
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
            [[ -n "${BREWFS_PREFETCH_ENABLED:-}" ]] && echo "  prefetch_enabled: ${BREWFS_PREFETCH_ENABLED}"
            [[ -n "${BREWFS_PREFETCH_MAX_BYTES:-}" ]] && echo "  prefetch_max_bytes: ${BREWFS_PREFETCH_MAX_BYTES}"
            [[ -n "${BREWFS_PREFETCH_CONCURRENCY:-}" ]] && echo "  prefetch_concurrency: ${BREWFS_PREFETCH_CONCURRENCY}"
            [[ -n "${BREWFS_MEMORY_BUDGET_BYTES:-}" ]] && echo "  memory_budget_bytes: ${BREWFS_MEMORY_BUDGET_BYTES}"
            [[ -n "${BREWFS_COMPRESSION:-}" ]] && echo "  compression: ${comp}"
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
    append_env_export BREWFS_FUSE_READ_DIRECT_IO
    append_env_export BREWFS_WRITEBACK_UPLOAD_CONCURRENCY
    append_env_export BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES
    append_env_export BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES
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
    export RUST_LOG="${RUST_LOG:-brewfs=info,rfuse3::raw::logfs=debug}"
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

stats_snapshot_after_tool() {
    local tool="$1"
    local stats_path="$mount_dir/.stats"
    {
        date -Iseconds
        echo
        if [[ -e "$stats_path" ]]; then
            tr -d '\000' <"$stats_path"
        else
            echo "missing $stats_path"
        fi
    } >"$artifact_dir/diagnostics/stats-${tool}-after.txt" 2>&1 || true
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
    local start end elapsed status

    start="$(date +%s)"
    info "运行压力工具: $tool"
    info "  命令: $*"
    redis_diag_before_tool "$tool"
    set +e
    if [[ "${PERF_LOG_TO_CONSOLE:-false}" == "true" ]]; then
        "$@" 2>&1 | tee "$log_path"
        status="${PIPESTATUS[0]}"
    else
        "$@" >"$log_path" 2>&1
        status=$?
    fi
    set -e
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

    require_tool_bin "$bin"
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

    require_tool_bin "$bin"
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

    require_tool_bin "$bin"
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

    require_tool_bin "$bin"
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
    local -a args=()

    profile_key="$(printf '%s' "$profile_suffix" | tr '[:lower:]-' '[:upper:]_')"
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
                direct="$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)"
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
                direct="$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)"
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
                direct="$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)"
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
                direct="$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)"
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
                direct="$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)"
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
                direct="$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)"
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
                direct="$(env_or_default "$direct_var" PERF_FIO_DIRECT 0)"
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
        prepare_fio_dataset "$tool" "$work_dir" "$name" "$size" "$direct" "$numjobs" "$bs" "$ioengine" "$iodepth" || return $?
    fi

    # Collect per-second latency logs for time-series analysis
    local lat_log_prefix="$artifact_dir/results/${tool}_lat"
    args+=(--output-format=json --output="$json_path")
    args+=(--write_lat_log="$lat_log_prefix" --log_avg_msec=1000)
    run_logged_tool "$tool" fio "${args[@]}"
    append_fio_log_summary "$json_path" "$artifact_dir/tools/${tool}.log" "$tool"
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

        lines.extend([
            "",
            "## Fio",
            "",
            "| Tool | Workload | BS | Jobs | Read BW | Read IOPS | Write BW | Write IOPS | Read P99 | Write P99 | Raw |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |---: | ---: | ---: |",
        ])

        for fio_json_path in fio_json_paths:
            data = json.loads(fio_json_path.read_text())
            jobs = data.get("jobs", [])
            if not jobs:
                continue
            options = first_job_options()
            read = op_totals("read")
            write = op_totals("write")
            tool_name = fio_json_path.stem
            lines.append(
                f"| {tool_name} | {options.get('rw', 'unknown')} | {options.get('bs', 'unknown')} | "
                f"{options.get('numjobs', 'unknown')} | {fmt_rate(read['bw_bytes'])} | "
                f"{fmt_iops(read['iops'])} | {fmt_rate(write['bw_bytes'])} | "
                f"{fmt_iops(write['iops'])} | {fmt_ms_from_ns(read['p99_ns'])} | "
                f"{fmt_ms_from_ns(write['p99_ns'])} | results/{fio_json_path.name} |"
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

# --- Metadata Performance ---
metaperf_log = artifact_dir / "tools" / "metaperf.log"
if metaperf_log.exists():
    try:
        lines.extend([
            "",
            "## Metadata Performance",
            "",
            "| Operation | Ops/sec | Latency (µs/op) |",
            "| --- | ---: | ---: |",
        ])
        for mline in metaperf_log.read_text().splitlines():
            if "ops/sec=" in mline and "usec/op" in mline:
                op = mline.split(":")[0].strip()
                ops_sec = mline.split("ops/sec=")[1].split(",")[0]
                usec_op = mline.split("usec/op")[1].strip().lstrip("= ")
                lines.append(f"| {op} | {float(ops_sec):.1f} | {float(usec_op):.0f} |")
    except Exception:
        pass

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
    lines.extend([
        "",
        "## BrewFS Stats",
        "",
        "| Tool | Cache hit | FUSE read | FUSE write | Dirty | Read buffer | S3 ops | Details |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
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

    for path in brewfs_stats_paths:
        tool = path.name.removeprefix("stats-").removesuffix("-after.txt")
        metrics = parse_brewfs_stats(path)
        hits = metrics.get("brewfs_cache_hits_total", 0.0)
        misses = metrics.get("brewfs_cache_misses_total", 0.0)
        requests = metrics.get("brewfs_cache_requests_total", hits + misses)
        hit_ratio = metrics.get(
            "brewfs_cache_hit_ratio",
            hits / requests if requests else 0.0,
        )
        fuse_read = metrics.get("brewfs_fuse_read_bytes_total", 0.0)
        fuse_write = metrics.get("brewfs_fuse_write_bytes_total", 0.0)
        dirty = metrics.get(
            "brewfs_writeback_dirty_bytes",
            metrics.get("brewfs_buffer_dirty_bytes", 0.0),
        )
        read_buffer = metrics.get(
            "brewfs_reader_buffer_bytes",
            metrics.get("brewfs_buffer_read_bytes", 0.0),
        )
        s3_get = metrics.get("brewfs_s3_get_ops_total", 0.0)
        s3_put = metrics.get("brewfs_s3_put_ops_total", 0.0)
        rel = path.relative_to(artifact_dir)
        lines.append(
            f"| {tool} | {hit_ratio * 100.0:.1f}% ({int(hits)}/{int(requests)}) | "
            f"{fmt_mib(fuse_read)} | {fmt_mib(fuse_write)} | {fmt_mib(dirty)} | "
            f"{fmt_mib(read_buffer)} | GET={int(s3_get)}, PUT={int(s3_put)} | {rel} |"
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
                    findings.append(f"- **{name}**: Write P99={p99:.0f}ms > 500ms — consider increasing write buffer or S3 concurrency.")

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
