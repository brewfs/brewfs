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
    mkdir -p "$artifact_dir/results" "$artifact_dir/tools"
    printf 'tool\tstatus\tseconds\tlog\n' >"$artifact_dir/perf-summary.tsv"
    write_juicefs_profile
}

truthy() {
    case "${1:-}" in
        1|true|TRUE|yes|YES|on|ON) return 0 ;;
        *) return 1 ;;
    esac
}

write_juicefs_profile() {
    cat >"$artifact_dir/juicefs-profile.env" <<EOF
JFS_COMPRESS=${jfs_compress}
JFS_WRITEBACK=${jfs_writeback}
JFS_BUFFER_SIZE_MIB=${jfs_buffer_size_mib}
JFS_CACHE_SIZE_MIB=${jfs_cache_size_mib}
JFS_MAX_UPLOADS=${jfs_max_uploads}
JFS_MAX_DOWNLOADS=${jfs_max_downloads}
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

run_logged_tool() {
    local tool="$1"
    shift
    local log_path="$artifact_dir/tools/${tool}.log"
    local start end elapsed status

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
    mkdir -p "$mount_dir"
    if mountpoint -q "$mount_dir" 2>/dev/null; then
        info "$mount_dir 已挂载，先卸载"
        umount "$mount_dir" 2>/dev/null || fusermount3 -u "$mount_dir" 2>/dev/null || true
    fi

    local -a mount_args=("$meta_url" "$mount_dir" --enable-xattr)

    if truthy "$jfs_writeback"; then
        mount_args+=(--writeback)
    fi
    [[ -n "$jfs_buffer_size_mib" ]] && mount_args+=(--buffer-size="$jfs_buffer_size_mib")
    [[ -n "$jfs_cache_size_mib" ]] && mount_args+=(--cache-size="$jfs_cache_size_mib")
    [[ -n "$jfs_max_uploads" ]] && mount_args+=(--max-uploads="$jfs_max_uploads")
    [[ -n "$jfs_max_downloads" ]] && mount_args+=(--max-downloads="$jfs_max_downloads")
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

    require_tool_bin "$bin"
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

    require_tool_bin "$bin"
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

    require_tool_bin "$bin"
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

    require_tool_bin "$bin"
    rm -rf "$work_dir"
    mkdir -p "$work_dir"

    if [[ -n "${PERF_LOOPTEST_ARGS:-}" ]]; then
        read -r -a args <<<"${PERF_LOOPTEST_ARGS}"
    else
        args=(-i "${PERF_LOOPTEST_ITERS:-200}" -o -r -w -t -f -s -v -b "${PERF_LOOPTEST_BUF_SIZE:-1048576}" "$loop_file")
    fi
    run_logged_tool looptest "$bin" "${args[@]}"
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
    fio "${prep_args[@]}" >"$prep_log" 2>&1
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
    local work_dir="$mount_dir/.perf-${tool}"
    local json_path="$artifact_dir/results/${tool}.json"
    local profile_suffix="${tool#fio-}"
    local profile_key=$(printf '%s' "$profile_suffix" | tr '[:lower:]-' '[:upper:]_')
    local profile_args_var="PERF_FIO_${profile_key}_ARGS"
    local name_var="PERF_FIO_${profile_key}_NAME"
    local rw_var="PERF_FIO_${profile_key}_RW"
    local rwmixread_var="PERF_FIO_${profile_key}_RWMIXREAD"
    local bs_var="PERF_FIO_${profile_key}_BS"
    local size_var="PERF_FIO_${profile_key}_SIZE"
    local numjobs_var="PERF_FIO_${profile_key}_NUMJOBS"
    local ioengine_var="PERF_FIO_${profile_key}_IOENGINE"
    local iodepth_var="PERF_FIO_${profile_key}_IODEPTH"
    local direct_var="PERF_FIO_${profile_key}_DIRECT"
    local runtime_var="PERF_FIO_${profile_key}_RUNTIME"

    local name rw rwmixread bs size numjobs ioengine iodepth direct runtime
    local needs_prefill=false
    local use_time_based=true
    local use_end_fsync=false
    local use_refill_buffers=false
    local -a args=()

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
        if truthy "${PERF_FIO_COLD_READ:-false}" || truthy "${PERF_FIO_PREFILL_DRAIN:-false}"; then
            info "同步 fio 预填充数据集: $tool"
            sync
        fi
        if truthy "${PERF_FIO_COLD_READ:-false}" || truthy "${PERF_FIO_PREFILL_REMOUNT:-false}"; then
            remount_juicefs_for_fio_profile "$tool"
        fi
    fi

    local lat_log_prefix="$artifact_dir/results/${tool}_lat"
    args+=(--output-format=json --output="$json_path")
    args+=(--write_lat_log="$lat_log_prefix" --log_avg_msec=1000)
    run_logged_tool "$tool" fio "${args[@]}"
    append_fio_log_summary "$json_path" "$artifact_dir/tools/${tool}.log" "$tool"
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

    if [[ "$status" -eq 0 ]]; then
        ok "性能测试全部完成"
    else
        err "性能测试存在失败项 (exit=$status)"
    fi

    return "$status"
}

main "$@"
