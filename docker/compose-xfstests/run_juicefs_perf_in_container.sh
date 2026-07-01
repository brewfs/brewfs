#!/usr/bin/env bash

set -euo pipefail

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

mount_dir="${JFS_MOUNT_POINT:-/mnt/juicefs}"
meta_url="${JFS_META_URL:-redis://redis:6379/0}"
s3_bucket="${JFS_S3_BUCKET:-brewfs-data}"
s3_endpoint="${JFS_S3_ENDPOINT:-http://rustfs:9000}"
s3_region="${JFS_S3_REGION:-us-east-1}"
access_key="${AWS_ACCESS_KEY_ID:-rustfsadmin}"
secret_key="${AWS_SECRET_ACCESS_KEY:-rustfsadmin}"
xfstests_dir="${XFSTESTS_DIR:-/opt/xfstests-dev}"
artifact_root="${BREWFS_ARTIFACT_ROOT:-/artifacts}"
artifact_dir="${BREWFS_ARTIFACT_DIR:-}"
perf_tools="${PERF_TOOLS:-fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest}"
jfs_compress="${JFS_COMPRESS:-none}"
jfs_writeback="${JFS_WRITEBACK:-false}"
jfs_buffer_size_mib="${JFS_BUFFER_SIZE_MIB:-}"
jfs_cache_size_mib="${JFS_CACHE_SIZE_MIB:-}"
jfs_max_uploads="${JFS_MAX_UPLOADS:-}"
jfs_max_downloads="${JFS_MAX_DOWNLOADS:-}"
jfs_open_cache="${JFS_OPEN_CACHE:-}"
jfs_open_cache_limit="${JFS_OPEN_CACHE_LIMIT:-}"
jfs_backup_meta="${JFS_BACKUP_META:-}"
jfs_no_usage_report="${JFS_NO_USAGE_REPORT:-false}"
jfs_cache_dir="${JFS_CACHE_DIR:-}"

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

prepare_artifacts() {
    mkdir -p "$artifact_dir/results" "$artifact_dir/tools" "$artifact_dir/diagnostics"
    printf 'tool\tstatus\tseconds\tlog\n' >"$artifact_dir/perf-summary.tsv"
    printf 'tool\tmethod\tops\tbytes\tseconds\tavg_ms\n' >"$artifact_dir/juicefs-object-summary.tsv"
    write_perf_profile
    printf 'tool\tpost_fio_drain_s\tstage_blocks\tstage_bytes\tuploading\tput_bytes\tget_bytes\n' \
        >"$artifact_dir/post-write-drain.tsv"
    write_juicefs_profile
}

truthy() {
    case "${1:-}" in
        1|true|TRUE|yes|YES|on|ON) return 0 ;;
        *) return 1 ;;
    esac
}

juicefs_mount_supports() {
    local option="$1"
    /usr/local/bin/juicefs mount --help 2>/dev/null | grep -q -- "$option"
}

write_perf_profile() {
    local path="$artifact_dir/perf-profile.env"
    local max_downloads_effective="$jfs_max_downloads"
    if [[ -n "$jfs_max_downloads" ]] && ! juicefs_mount_supports "--max-downloads"; then
        max_downloads_effective="unsupported"
    fi
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
PERF_FIO_DIRECT_MATRIX=${PERF_FIO_DIRECT_MATRIX:-}
JFS_COMPRESS=${jfs_compress}
JFS_WRITEBACK=${jfs_writeback}
JFS_BUFFER_SIZE_MIB=${jfs_buffer_size_mib}
JFS_CACHE_SIZE_MIB=${jfs_cache_size_mib}
JFS_MAX_UPLOADS=${jfs_max_uploads}
JFS_MAX_DOWNLOADS=${jfs_max_downloads}
JFS_MAX_DOWNLOADS_EFFECTIVE=${max_downloads_effective}
JFS_OPEN_CACHE=${jfs_open_cache}
JFS_OPEN_CACHE_LIMIT=${jfs_open_cache_limit}
JFS_BACKUP_META=${jfs_backup_meta}
JFS_NO_USAGE_REPORT=${jfs_no_usage_report}
JFS_CACHE_DIR=${jfs_cache_dir}
EOF

    {
        echo
        echo "# Raw PERF_FIO environment"
        env | sort | grep '^PERF_FIO_' || true
    } >>"$path"
}

write_juicefs_profile() {
    local max_downloads_effective="$jfs_max_downloads"
    if [[ -n "$jfs_max_downloads" ]] && ! juicefs_mount_supports "--max-downloads"; then
        max_downloads_effective="unsupported"
    fi
    cat >"$artifact_dir/juicefs-profile.env" <<EOF
JFS_COMPRESS=${jfs_compress}
JFS_WRITEBACK=${jfs_writeback}
JFS_BUFFER_SIZE_MIB=${jfs_buffer_size_mib}
JFS_CACHE_SIZE_MIB=${jfs_cache_size_mib}
JFS_MAX_UPLOADS=${jfs_max_uploads}
JFS_MAX_DOWNLOADS=${jfs_max_downloads}
JFS_MAX_DOWNLOADS_EFFECTIVE=${max_downloads_effective}
JFS_OPEN_CACHE=${jfs_open_cache}
JFS_OPEN_CACHE_LIMIT=${jfs_open_cache_limit}
JFS_BACKUP_META=${jfs_backup_meta}
JFS_NO_USAGE_REPORT=${jfs_no_usage_report}
JFS_CACHE_DIR=${jfs_cache_dir}
EOF
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

scrape_juicefs_metrics() {
    local label="$1"
    local out="$artifact_dir/diagnostics/juicefs-metrics-${label}.txt"

    if command -v curl >/dev/null 2>&1; then
        curl -fsS --max-time 2 "http://127.0.0.1:9567/metrics" >"$out" 2>/dev/null || : >"$out"
    else
        : >"$out"
    fi

    printf '%s' "$out"
}

metric_value() {
    local file="$1"
    local metric="$2"
    local method="$3"

    if [[ ! -s "$file" ]]; then
        printf '0'
        return
    fi

    awk -v metric="$metric" -v method="$method" '
        $1 ~ "^" metric "\\{" && $0 ~ "method=\"" method "\"" { total += $NF }
        END { printf "%.6f", total + 0 }
    ' "$file"
}

append_juicefs_object_summary() {
    local tool="$1"
    local before="$2"
    local after="$3"
    local method

    for method in GET PUT DELETE; do
        local before_ops after_ops ops
        local before_seconds after_seconds seconds
        local before_bytes after_bytes bytes
        local avg_ms

        before_ops="$(metric_value "$before" juicefs_object_request_durations_histogram_seconds_count "$method")"
        after_ops="$(metric_value "$after" juicefs_object_request_durations_histogram_seconds_count "$method")"
        before_seconds="$(metric_value "$before" juicefs_object_request_durations_histogram_seconds_sum "$method")"
        after_seconds="$(metric_value "$after" juicefs_object_request_durations_histogram_seconds_sum "$method")"
        before_bytes="$(metric_value "$before" juicefs_object_request_data_bytes "$method")"
        after_bytes="$(metric_value "$after" juicefs_object_request_data_bytes "$method")"

        ops="$(awk -v a="$after_ops" -v b="$before_ops" 'BEGIN { printf "%.0f", a - b }')"
        seconds="$(awk -v a="$after_seconds" -v b="$before_seconds" 'BEGIN { printf "%.6f", a - b }')"
        bytes="$(awk -v a="$after_bytes" -v b="$before_bytes" 'BEGIN { printf "%.0f", a - b }')"
        avg_ms="$(awk -v s="$seconds" -v n="$ops" 'BEGIN { if (n > 0) printf "%.3f", (s / n) * 1000; else printf "0.000" }')"

        printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$tool" "$method" "$ops" "$bytes" "$seconds" "$avg_ms" >>"$artifact_dir/juicefs-object-summary.tsv"
    done
}

run_logged_tool() {
    local tool="$1"
    shift
    local log_path="$artifact_dir/tools/${tool}.log"
    local start end elapsed status
    local metrics_before metrics_after

    metrics_before="$(scrape_juicefs_metrics "${tool}-before")"
    start="$(date +%s)"
    info "运行压力工具: $tool"
    info "  命令: $*"
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
    metrics_after="$(scrape_juicefs_metrics "${tool}-after")"
    append_juicefs_object_summary "$tool" "$metrics_before" "$metrics_after"

    local log_size
    log_size=$(wc -c < "$log_path" 2>/dev/null || echo 0)

    if [[ "$status" -eq 0 ]]; then
        ok "压力工具完成: $tool (${elapsed}s, log=${log_size} bytes)"
        printf '%s\tpass\t%s\t%s\n' "$tool" "$elapsed" "$log_path" >>"$artifact_dir/perf-summary.tsv"
    else
        err "压力工具失败: $tool (exit=$status, ${elapsed}s, log=${log_size} bytes)"
        printf '%s\tfail(%s)\t%s\t%s\n' "$tool" "$status" "$elapsed" "$log_path" >>"$artifact_dir/perf-summary.tsv"
        if [[ -s "$log_path" ]]; then
            err "  最后几行日志:"
            grep -v '^$' "$log_path" | tail -5 | while read -r line; do
                err "    $line"
            done
        fi
    fi

    return "$status"
}

format_juicefs() {
    info "检查 JuiceFS 是否已格式化: $meta_url"
    if /usr/local/bin/juicefs status "$meta_url" >/dev/null 2>&1; then
        info "JuiceFS 已格式化，跳过 format"
        return 0
    fi

    # JuiceFS uses bucket URL to specify custom S3 endpoint:
    #   http://<endpoint>/<bucket>
    local bucket_url="${s3_endpoint}/${s3_bucket}"

    info "格式化 JuiceFS: $meta_url (bucket=$bucket_url)"
    /usr/local/bin/juicefs format \
        --storage s3 \
        --bucket "$bucket_url" \
        --access-key "$access_key" \
        --secret-key "$secret_key" \
        --compress "$jfs_compress" \
        "$meta_url" \
        myjfs

    ok "JuiceFS 格式化完成"
}

mount_juicefs() {
    local effective_writeback="${1:-$jfs_writeback}"

    mkdir -p "$mount_dir"
    if mountpoint -q "$mount_dir" 2>/dev/null; then
        info "$mount_dir 已挂载，先卸载"
        umount "$mount_dir" 2>/dev/null || fusermount3 -u "$mount_dir" 2>/dev/null || true
    fi

    local -a mount_args=("$meta_url" "$mount_dir" --enable-xattr)

    if truthy "$effective_writeback"; then
        mount_args+=(--writeback)
    fi
    [[ -n "$jfs_buffer_size_mib" ]] && mount_args+=(--buffer-size="$jfs_buffer_size_mib")
    [[ -n "$jfs_cache_size_mib" ]] && mount_args+=(--cache-size="$jfs_cache_size_mib")
    [[ -n "$jfs_max_uploads" ]] && mount_args+=(--max-uploads="$jfs_max_uploads")
    if [[ -n "$jfs_max_downloads" ]]; then
        if juicefs_mount_supports "--max-downloads"; then
            mount_args+=(--max-downloads="$jfs_max_downloads")
        else
            err "当前 JuiceFS 不支持 --max-downloads，跳过 JFS_MAX_DOWNLOADS=${jfs_max_downloads}"
        fi
    fi
    [[ -n "$jfs_open_cache" ]] && mount_args+=(--open-cache="$jfs_open_cache")
    [[ -n "$jfs_open_cache_limit" ]] && mount_args+=(--open-cache-limit="$jfs_open_cache_limit")
    [[ -n "$jfs_backup_meta" ]] && mount_args+=(--backup-meta="$jfs_backup_meta")
    [[ -n "$jfs_cache_dir" ]] && mount_args+=(--cache-dir="$jfs_cache_dir")
    if truthy "$jfs_no_usage_report"; then
        mount_args+=(--no-usage-report)
    fi
    mount_args+=(-o allow_other)

    info "挂载 JuiceFS: /usr/local/bin/juicefs mount ${mount_args[*]}"
    /usr/local/bin/juicefs mount "${mount_args[@]}" &

    local i=0
    for ((i = 0; i < 30; i++)); do
        if mountpoint -q "$mount_dir" 2>/dev/null; then
            ok "JuiceFS 已挂载"
            return 0
        fi
        sleep 1
    done

    err "JuiceFS 挂载失败: $mount_dir"
    exit 1
}

# ---- perf tool runners (same logic as brewfs) ----

drop_kernel_page_cache_if_requested() {
    if truthy "${PERF_FIO_DROP_CACHES:-false}" || truthy "${PERF_FIO_COLD_READ_DROP_CACHES:-false}"; then
        info "请求 drop_caches 以降低页缓存影响"
        sync || true
        if ! sh -c 'echo 3 > /proc/sys/vm/drop_caches' >/dev/null 2>&1; then
            err "drop_caches 失败；继续测试，但结果可能仍受页缓存影响"
        fi
    fi
}

clear_juicefs_cache_if_requested() {
    if truthy "${PERF_FIO_COLD_READ:-false}" || truthy "${PERF_FIO_COLD_READ_CLEAR_CACHE:-false}"; then
        local root="${jfs_cache_dir:-}"
        if [[ -n "$root" && "$root" == /* && "$root" != "/" ]]; then
            info "清理 JuiceFS 本地 cache dir: $root"
            rm -rf -- "$root"
        else
            err "跳过 JuiceFS cache dir 清理，路径不安全: ${root:-<empty>}"
        fi
    fi
}

juicefs_stats_path() {
    if [[ -e "$mount_dir/.jfs.stats" ]]; then
        printf '%s/.jfs.stats' "$mount_dir"
    elif [[ -e "$mount_dir/.stats" ]]; then
        printf '%s/.stats' "$mount_dir"
    else
        return 1
    fi
}

stats_snapshot_after_tool() {
    local tool="$1"
    local stats_path
    {
        date -Iseconds
        echo
        if stats_path="$(juicefs_stats_path 2>/dev/null)"; then
            tr -d '\000' <"$stats_path"
        else
            echo "missing JuiceFS stats file under $mount_dir"
        fi
    } >"$artifact_dir/diagnostics/juicefs-stats-${tool}-after.txt" 2>&1 || true
}

stats_snapshot_before_tool() {
    local tool="$1"
    local stats_path
    {
        date -Iseconds
        echo
        if stats_path="$(juicefs_stats_path 2>/dev/null)"; then
            tr -d '\000' <"$stats_path"
        else
            echo "missing JuiceFS stats file under $mount_dir"
        fi
    } >"$artifact_dir/diagnostics/juicefs-stats-${tool}-before.txt" 2>&1 || true
}

juicefs_stat_value() {
    local metric="$1"
    local stats_path
    stats_path="$(juicefs_stats_path)" || return 1

    tr -d '\000' <"$stats_path" | awk -v metric="$metric" '
        NF >= 2 {
            name = $1
            sub(/\{.*/, "", name)
            if (name == metric && $2 ~ /^[-+]?[0-9.]+([eE][-+]?[0-9]+)?$/) {
                sum += $2
                found = 1
            }
        }
        END {
            if (found) {
                printf "%.0f\n", sum
            } else {
                exit 1
            }
        }
    '
}

numeric_juicefs_stat_or_zero() {
    local metric="$1"
    local value
    value="$(juicefs_stat_value "$metric" 2>/dev/null || true)"
    if [[ "$value" =~ ^[0-9]+$ ]]; then
        printf '%s' "$value"
    else
        printf '0'
    fi
}

wait_for_fio_prefill_drain() {
    local tool="$1"
    local timeout="${PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS:-600}"
    local interval="${PERF_FIO_PREFILL_DRAIN_INTERVAL_SECS:-2}"
    local threshold="${PERF_FIO_PREFILL_DRAIN_PENDING_BYTES:-0}"
    local start now elapsed staged_blocks staged_bytes uploading put_bytes get_bytes

    info "等待 JuiceFS fio 预填充写回完成: $tool (stage_bytes<=${threshold}, timeout=${timeout}s)"
    sync || true
    start="$(date +%s)"

    while true; do
        staged_blocks="$(numeric_juicefs_stat_or_zero juicefs_staging_blocks)"
        staged_bytes="$(numeric_juicefs_stat_or_zero juicefs_staging_block_bytes)"
        uploading="$(numeric_juicefs_stat_or_zero juicefs_object_request_uploading)"
        put_bytes="$(numeric_juicefs_stat_or_zero juicefs_object_request_data_bytes_PUT)"
        get_bytes="$(numeric_juicefs_stat_or_zero juicefs_object_request_data_bytes_GET)"

        now="$(date +%s)"
        elapsed="$((now - start))"

        if (( staged_bytes <= threshold && staged_blocks == 0 && uploading == 0 )); then
            ok "JuiceFS 预填充写回已完成: $tool (stage_blocks=$staged_blocks stage_bytes=$staged_bytes uploading=$uploading put_bytes=$put_bytes get_bytes=$get_bytes elapsed=${elapsed}s)"
            stats_snapshot_after_tool "${tool}-prefill-drained"
            return 0
        fi

        if (( elapsed >= timeout )); then
            err "JuiceFS 预填充写回等待超时: $tool (stage_blocks=$staged_blocks stage_bytes=$staged_bytes uploading=$uploading put_bytes=$put_bytes get_bytes=$get_bytes elapsed=${elapsed}s)"
            stats_snapshot_after_tool "${tool}-prefill-drain-timeout"
            return 1
        fi

        if (( elapsed % 10 == 0 )); then
            info "  JuiceFS 写回等待中: stage_blocks=$staged_blocks stage_bytes=$staged_bytes uploading=$uploading put_bytes=$put_bytes get_bytes=$get_bytes elapsed=${elapsed}s"
        fi
        sleep "$interval"
    done
}

wait_for_fio_post_write_drain() {
    local tool="$1"
    local timeout="${PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS:-600}"
    local interval="${PERF_FIO_POST_WRITE_DRAIN_INTERVAL_SECS:-2}"
    local threshold="${PERF_FIO_POST_WRITE_DRAIN_PENDING_BYTES:-0}"
    local start now elapsed staged_blocks staged_bytes uploading put_bytes get_bytes

    truthy "${PERF_FIO_POST_WRITE_DRAIN:-false}" || return 0
    case "$tool" in
        fio-seqwrite*|fio-randwrite*|fio-randrw*|fio-bigwrite*) ;;
        *) return 0 ;;
    esac

    stats_snapshot_before_tool "${tool}-post-write-drained"
    stats_snapshot_before_tool "${tool}-post-write-drain-timeout"
    start="$(date +%s)"
    info "等待 JuiceFS fio 写入后写回完成: $tool (stage_bytes<=${threshold}, timeout=${timeout}s)"
    sync || true

    while true; do
        staged_blocks="$(numeric_juicefs_stat_or_zero juicefs_staging_blocks)"
        staged_bytes="$(numeric_juicefs_stat_or_zero juicefs_staging_block_bytes)"
        uploading="$(numeric_juicefs_stat_or_zero juicefs_object_request_uploading)"
        put_bytes="$(numeric_juicefs_stat_or_zero juicefs_object_request_data_bytes_PUT)"
        get_bytes="$(numeric_juicefs_stat_or_zero juicefs_object_request_data_bytes_GET)"

        now="$(date +%s)"
        elapsed="$((now - start))"

        if (( staged_bytes <= threshold && staged_blocks == 0 && uploading == 0 )); then
            ok "JuiceFS fio 写入后写回已完成: $tool (stage_blocks=$staged_blocks stage_bytes=$staged_bytes uploading=$uploading put_bytes=$put_bytes get_bytes=$get_bytes elapsed=${elapsed}s)"
            printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$tool" "$elapsed" "$staged_blocks" "$staged_bytes" "$uploading" "$put_bytes" "$get_bytes" \
                >>"$artifact_dir/post-write-drain.tsv"
            stats_snapshot_after_tool "${tool}-post-write-drained"
            return 0
        fi

        if (( elapsed >= timeout )); then
            err "JuiceFS fio 写入后写回等待超时: $tool (stage_blocks=$staged_blocks stage_bytes=$staged_bytes uploading=$uploading put_bytes=$put_bytes get_bytes=$get_bytes elapsed=${elapsed}s)"
            printf '%s\ttimeout:%s\t%s\t%s\t%s\t%s\t%s\n' "$tool" "$elapsed" "$staged_blocks" "$staged_bytes" "$uploading" "$put_bytes" "$get_bytes" \
                >>"$artifact_dir/post-write-drain.tsv"
            stats_snapshot_after_tool "${tool}-post-write-drain-timeout"
            return 1
        fi

        if (( elapsed % 10 == 0 )); then
            info "  JuiceFS 写后写回等待中: stage_blocks=$staged_blocks stage_bytes=$staged_bytes uploading=$uploading put_bytes=$put_bytes get_bytes=$get_bytes elapsed=${elapsed}s"
        fi
        sleep "$interval"
    done
}

remount_juicefs_for_fio_profile() {
    local tool="$1"

    info "为 fio cold-read 重挂载 JuiceFS: $tool"
    sync || true
    cleanup
    clear_juicefs_cache_if_requested
    drop_kernel_page_cache_if_requested
    mount_juicefs
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
        args=(-d "$work_dir" -p "${PERF_DIRSTRESS_PROCS:-4}" -f "${PERF_DIRSTRESS_FILES:-200}" -n "${PERF_DIRSTRESS_PROCS_PER_DIR:-2}" -s "${PERF_DIRSTRESS_SEED:-1}")
    fi
    run_logged_tool dirstress "$bin" "${args[@]}"
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
        args=(-d "$work_dir" -a "${PERF_DIRPERF_ADDSTEP:-100}" -f "${PERF_DIRPERF_FIRST:-100}" -l "${PERF_DIRPERF_LAST:-1000}" -c "${PERF_DIRPERF_NAME_LEN:-16}" -n "${PERF_DIRPERF_DIRS:-2}" -s "${PERF_DIRPERF_STATS:-5}")
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
        args=(-d "$work_dir" -t "${PERF_METAPERF_SECONDS:-30}" -s "${PERF_METAPERF_FILE_SIZE:-4096}" -l "${PERF_METAPERF_NAME_LEN:-16}" -L "${PERF_METAPERF_BG_NAME_LEN:-16}" -n "${PERF_METAPERF_OP_FILES:-200}" -N "${PERF_METAPERF_BG_FILES:-2000}" create open stat readdir rename)
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
        args=(-i "${PERF_LOOPTEST_ITERS:-200}" -o -r -w -t -f -s -v -b "${PERF_LOOPTEST_BUF_SIZE:-1048576}" "$loop_file")
    fi
    run_logged_tool looptest "$bin" "${args[@]}"
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
    local durable_prefill=false
    local status=0
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

    if truthy "$jfs_writeback" && {
        truthy "${PERF_FIO_COLD_READ:-false}" \
            || truthy "${PERF_FIO_PREFILL_REMOUNT:-false}" \
            || truthy "${PERF_FIO_COLD_READ_CLEAR_CACHE:-false}";
    }; then
        durable_prefill=true
    fi

    if [[ "$durable_prefill" == true ]]; then
        info "为 cold-read 预填充临时关闭 JuiceFS writeback: $tool"
        cleanup
        mount_juicefs false
        mkdir -p "$work_dir"
    fi

    info "预填充 fio 数据集: $tool"
    fio "${prep_args[@]}" >"$prep_log" 2>&1 || status=$?

    if [[ "$durable_prefill" == true ]]; then
        info "同步 durable 预填充并恢复 JuiceFS writeback: $tool"
        sync || true
        cleanup
        mount_juicefs
    fi

    return "$status"
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
" >> "$log_path" 2>/dev/null || true
    fi
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
        prepare_fio_dataset "$tool" "$work_dir" "$name" "$size" "$direct" "$numjobs" "$bs" "$ioengine" "$iodepth" || return $?
        stats_snapshot_after_tool "${tool}-prefill"
        if truthy "${PERF_FIO_COLD_READ:-false}" || truthy "${PERF_FIO_PREFILL_DRAIN:-false}"; then
            wait_for_fio_prefill_drain "$tool" || return $?
        fi
        if truthy "${PERF_FIO_COLD_READ:-false}" || truthy "${PERF_FIO_PREFILL_REMOUNT:-false}"; then
            remount_juicefs_for_fio_profile "$tool" || return $?
        fi
    fi

    local lat_log_prefix="$artifact_dir/results/${tool}_lat"
    args+=(--output-format=json --output="$json_path")
    args+=(--write_lat_log="$lat_log_prefix" --log_avg_msec=1000)
    run_logged_tool "$tool" fio "${args[@]}"
    append_fio_log_summary "$json_path" "$artifact_dir/tools/${tool}.log" "$tool"
    wait_for_fio_post_write_drain "$tool"
}

generate_perf_report() {
    python3 - "$artifact_dir" <<'PY'
import csv
import json
import pathlib
import sys

artifact_dir = pathlib.Path(sys.argv[1])
summary_path = artifact_dir / "perf-summary.tsv"
profile_path = artifact_dir / "juicefs-profile.env"
post_write_drain_path = artifact_dir / "post-write-drain.tsv"
report_path = artifact_dir / "report.md"
fio_json_paths = sorted((artifact_dir / "results").glob("fio*.json"))

rows = []
if summary_path.exists():
    with summary_path.open(newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
summary_by_tool = {row.get("tool", ""): row for row in rows}

lines = [
    "# JuiceFS Perf Report",
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

if profile_path.exists():
    lines.extend([
        "",
        "## JuiceFS Profile",
        "",
        "| Key | Value |",
        "| --- | --- |",
    ])
    for raw in profile_path.read_text(errors="replace").splitlines():
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        lines.append(f"| {key} | {value} |")

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
            "| Tool | Drain seconds | Stage blocks | Stage bytes | Uploading | Put bytes | Get bytes |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ])
        for row in drain_rows:
            lines.append(
                f"| {row.get('tool', '')} | {row.get('post_fio_drain_s', '')} | "
                f"{row.get('stage_blocks', '')} | {row.get('stage_bytes', '')} | "
                f"{row.get('uploading', '')} | {row.get('put_bytes', '')} | "
                f"{row.get('get_bytes', '')} |"
            )

if fio_json_paths:
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

    lines.extend([
        "",
        "## Fio",
        "",
        "| Tool | Workload | Direct | BS | Jobs | Read BW | Read IOPS | Write BW | Write IOPS | Read P99 | Write P99 | Raw |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])

    runtime_rows = []
    for fio_json_path in fio_json_paths:
        data = json.loads(fio_json_path.read_text())
        jobs = data.get("jobs", [])
        if not jobs:
            continue
        options = next((job.get("job options", {}) for job in jobs if job.get("job options")), {})

        def op_totals(op_name):
            ops = [job.get(op_name, {}) for job in jobs]
            runtimes = [num(op.get("runtime")) for op in ops if num(op.get("runtime")) > 0]
            return {
                "bw_bytes": sum(num(op.get("bw_bytes")) for op in ops),
                "iops": sum(num(op.get("iops")) for op in ops),
                "runtime_ms": max(runtimes) if runtimes else 0,
                "p99_ns": max((num(latency_percentile(op, 99)) for op in ops), default=0),
            }

        read = op_totals("read")
        write = op_totals("write")
        tool_name = fio_json_path.stem
        wall_seconds = num(summary_by_tool.get(tool_name, {}).get("seconds"))
        active_runtime_ms = max(read["runtime_ms"], write["runtime_ms"])
        runtime_rows.append((tool_name, options.get("direct", "unknown"), wall_seconds, active_runtime_ms))
        lines.append(
            f"| {tool_name} | {options.get('rw', 'unknown')} | {options.get('direct', 'unknown')} | "
            f"{options.get('bs', 'unknown')} | {options.get('numjobs', 'unknown')} | "
            f"{fmt_rate(read['bw_bytes'])} | {fmt_iops(read['iops'])} | "
            f"{fmt_rate(write['bw_bytes'])} | {fmt_iops(write['iops'])} | "
            f"{fmt_ms_from_ns(read['p99_ns'])} | {fmt_ms_from_ns(write['p99_ns'])} | "
            f"results/{fio_json_path.name} |"
        )

    if runtime_rows:
        lines.extend([
            "",
            "## Fio Runtime Accounting",
            "",
            "| Tool | Direct | Script wall | active_io_runtime | wall-active_io |",
            "| --- | ---: | ---: | ---: | ---: |",
        ])
        for tool_name, direct, wall_seconds, active_runtime_ms in runtime_rows:
            active_seconds = active_runtime_ms / 1000.0 if active_runtime_ms else 0.0
            delta = wall_seconds - active_seconds if wall_seconds and active_seconds else 0.0
            lines.append(
                f"| {tool_name} | {direct} | {wall_seconds:.0f} s | "
                f"{active_seconds:.3f} s | {delta:+.3f} s |"
            )

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
            dirstress)    run_dirstress || status=1 ;;
            dirperf)      run_dirperf || status=1 ;;
            metaperf)     run_metaperf || status=1 ;;
            looptest)     run_looptest || status=1 ;;
            stress-ng)    run_stress_ng || status=1 ;;
            fio)          run_fio_custom || status=1 ;;
            fio-seqread)  run_fio_profile "$tool" seqread || status=1 ;;
            fio-seqwrite) run_fio_profile "$tool" seqwrite || status=1 ;;
            fio-randread) run_fio_profile "$tool" randread || status=1 ;;
            fio-randwrite) run_fio_profile "$tool" randwrite || status=1 ;;
            fio-randrw)   run_fio_profile "$tool" randrw || status=1 ;;
            fio-bigwrite) run_fio_profile "$tool" bigwrite || status=1 ;;
            fio-bigread)  run_fio_profile "$tool" bigread || status=1 ;;
            *)
                err "不支持的 PERF_TOOLS 项: $tool"
                status=1
                ;;
        esac
    done

    return "$status"
}

cleanup() {
    while mountpoint -q "$mount_dir" 2>/dev/null; do
        umount "$mount_dir" 2>/dev/null || fusermount3 -u "$mount_dir" 2>/dev/null || umount -l "$mount_dir" 2>/dev/null || sleep 1
    done
    pkill -f "juicefs mount" 2>/dev/null || true
}

on_exit() {
    local s=$?
    cleanup || true
    exit "$s"
}

main() {
    if [[ -z "$artifact_dir" ]]; then
        local ts
        ts="$(date +%s)-$RANDOM"
        artifact_dir="${artifact_root%/}/perf-run-${ts}"
    fi

    mkdir -p "$artifact_dir"
    chmod a+rwx "$artifact_dir" >/dev/null 2>&1 || true

    trap on_exit EXIT INT TERM

    info "准备产物目录: $artifact_dir"
    prepare_artifacts

    format_juicefs
    mount_juicefs

    # Pre-flight check
    info "执行挂载点预检: $mount_dir"
    local preflight_dir="$mount_dir/.perf-preflight"
    local preflight_file="$preflight_dir/test.bin"
    rm -rf "$preflight_dir"
    mkdir -p "$preflight_dir"
    if ! echo "juicefs-preflight-$(date +%s)" > "$preflight_file"; then
        err "预检失败: 无法写入 $preflight_file"
        exit 1
    fi
    ok "预检通过: 写入/读取正常"
    rm -rf "$preflight_dir"

    info "开始性能测试: tools=$perf_tools"
    set +e
    run_perf_suite
    local status=$?
    set -e
    generate_perf_report || true

    if [[ "$status" -eq 0 ]]; then
        ok "性能测试全部完成"
    else
        err "性能测试存在失败项 (exit=$status)"
    fi

    return "$status"
}

main "$@"
