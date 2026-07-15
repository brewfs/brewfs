#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

COMPOSE_FILE="$SCRIPT_DIR/docker-compose.juicefs-perf.yml"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

usage() {
    cat <<EOF
用法: $(basename "$0") [选项]

说明:
  - 使用 docker compose 在容器内运行 JuiceFS + xfstests 压力工具
  - 元数据库为 redis，对象存储为 rustfs
  - 测试产物输出到: $ARTIFACTS_DIR/perf-run-*

选项:
  --cached-read-throughput-profile
                             启用 JuiceFS 热缓存并发/随机读 profile（同步上传, buffer=4096MiB, cache=8192MiB, cache-large-write, readahead=1024MiB, prefetch=4, open-cache=1s/65536）；顺序流式读应使用默认 profile
  --metadata-throughput-profile
                             启用 JuiceFS metadata profile（open-cache=1s/65536, backup-meta=0, no-usage-report）
  --writeback-throughput-profile
                             启用 JuiceFS 对齐吞吐 profile（writeback, buffer=8192MiB, cache=4096MiB, upload/download concurrency=4/16, open-cache=1s/65536, compression=none, backup-meta=0, fio prefill staging drain+remount, write fio post-drain）
  --tools "<tool...>"        指定压力工具列表，默认: "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"
  --keep                     结束后不执行 compose down（便于调试）
  -h, --help                 显示帮助

支持的 PERF_TOOLS:
  fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw fio dirstress dirperf metaperf looptest stress-ng

可通过环境变量覆盖各工具参数:
  PERF_DIRSTRESS_ARGS PERF_DIRPERF_ARGS PERF_METAPERF_ARGS PERF_LOOPTEST_ARGS
  PERF_STRESS_NG_ARGS 可完全覆盖默认 stress-ng 参数；如需 link/symlink stressor 请用该变量显式指定
  PERF_FIO_ARGS PERF_FIO_RUNTIME PERF_FIO_SIZE PERF_FIO_BS PERF_FIO_NUMJOBS PERF_FIO_DIRECT
  PERF_FIO_DIRECT_MATRIX="0 1" 可对 fio profile 显式跑 buffered/direct 矩阵（默认不启用）
  PERF_FIO_{SEQREAD,SEQWRITE,RANDREAD,RANDWRITE,RANDRW,BIGREAD,BIGWRITE}_{ARGS,BS,SIZE,NUMJOBS,IOENGINE,IODEPTH,DIRECT,DIRECT_MATRIX,RUNTIME}
  PERF_FIO_COLD_READ PERF_FIO_PREFILL_DRAIN PERF_FIO_PREFILL_REMOUNT PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS PERF_FIO_PREFILL_DRAIN_INTERVAL_SECS PERF_FIO_PREFILL_DRAIN_PENDING_BYTES
  PERF_FIO_POST_WRITE_DRAIN PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS PERF_FIO_POST_WRITE_DRAIN_INTERVAL_SECS PERF_FIO_POST_WRITE_DRAIN_PENDING_BYTES
  PERF_FIO_DROP_CACHES PERF_FIO_COLD_READ_DROP_CACHES PERF_FIO_COLD_READ_CLEAR_CACHE
  JFS_COMPRESS JFS_WRITEBACK JFS_BUFFER_SIZE_MIB JFS_CACHE_SIZE_MIB JFS_CACHE_LARGE_WRITE
  JFS_MAX_UPLOADS JFS_MAX_DOWNLOADS JFS_MAX_READAHEAD_MIB JFS_PREFETCH
  JFS_OPEN_CACHE JFS_OPEN_CACHE_LIMIT JFS_BACKUP_META JFS_NO_USAGE_REPORT JFS_CACHE_DIR
  REDIS_PERF_DATA_MOUNT 可把 Redis AOF/RDB 数据挂到大容量目录或命名卷（例如 /data/slayer/juicefs-perf-redis）
  PERF_LOG_TO_CONSOLE=true 可恢复压测工具日志输出到终端（默认关闭）
EOF
    exit 0
}

require_value() {
    local option="$1"
    local value="${2:-}"
    if [[ -z "$value" ]]; then
        err "$option 需要提供参数值"
        exit 1
    fi
}

KEEP=false
CACHED_READ_THROUGHPUT_PROFILE=false
METADATA_THROUGHPUT_PROFILE=false
WRITEBACK_THROUGHPUT_PROFILE=false
PERF_TOOLS_VALUE="fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --cached-read-throughput-profile)
            CACHED_READ_THROUGHPUT_PROFILE=true
            shift
            ;;
        --metadata-throughput-profile)
            METADATA_THROUGHPUT_PROFILE=true
            shift
            ;;
        --writeback-throughput-profile)
            WRITEBACK_THROUGHPUT_PROFILE=true
            shift
            ;;
        --tools)
            require_value "$1" "${2:-}"
            PERF_TOOLS_VALUE="${2:-}"
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
            err "未知参数: $1"
            usage
            ;;
    esac
done

if [[ "$METADATA_THROUGHPUT_PROFILE" == true ]]; then
    export JFS_OPEN_CACHE="${JFS_OPEN_CACHE:-1s}"
    export JFS_OPEN_CACHE_LIMIT="${JFS_OPEN_CACHE_LIMIT:-65536}"
    export JFS_BACKUP_META="${JFS_BACKUP_META:-0}"
    export JFS_NO_USAGE_REPORT="${JFS_NO_USAGE_REPORT:-true}"
fi

if [[ "$CACHED_READ_THROUGHPUT_PROFILE" == true ]]; then
    export JFS_COMPRESS="${JFS_COMPRESS:-none}"
    export JFS_WRITEBACK="${JFS_WRITEBACK:-false}"
    export JFS_BUFFER_SIZE_MIB="${JFS_BUFFER_SIZE_MIB:-4096}"
    export JFS_CACHE_SIZE_MIB="${JFS_CACHE_SIZE_MIB:-8192}"
    export JFS_CACHE_LARGE_WRITE="${JFS_CACHE_LARGE_WRITE:-true}"
    export JFS_MAX_UPLOADS="${JFS_MAX_UPLOADS:-20}"
    export JFS_MAX_READAHEAD_MIB="${JFS_MAX_READAHEAD_MIB:-1024}"
    export JFS_PREFETCH="${JFS_PREFETCH:-4}"
    export JFS_OPEN_CACHE="${JFS_OPEN_CACHE:-1s}"
    export JFS_OPEN_CACHE_LIMIT="${JFS_OPEN_CACHE_LIMIT:-65536}"
    export JFS_BACKUP_META="${JFS_BACKUP_META:-0}"
    export JFS_NO_USAGE_REPORT="${JFS_NO_USAGE_REPORT:-true}"
    export JFS_CACHE_DIR="${JFS_CACHE_DIR:-/var/lib/juicefs/cache}"
fi

if [[ "$WRITEBACK_THROUGHPUT_PROFILE" == true ]]; then
    export JFS_COMPRESS="${JFS_COMPRESS:-none}"
    export JFS_WRITEBACK="${JFS_WRITEBACK:-true}"
    export JFS_BUFFER_SIZE_MIB="${JFS_BUFFER_SIZE_MIB:-8192}"
    export JFS_CACHE_SIZE_MIB="${JFS_CACHE_SIZE_MIB:-4096}"
    export JFS_MAX_UPLOADS="${JFS_MAX_UPLOADS:-4}"
    export JFS_MAX_DOWNLOADS="${JFS_MAX_DOWNLOADS:-16}"
    export JFS_OPEN_CACHE="${JFS_OPEN_CACHE:-1s}"
    export JFS_OPEN_CACHE_LIMIT="${JFS_OPEN_CACHE_LIMIT:-65536}"
    export JFS_BACKUP_META="${JFS_BACKUP_META:-0}"
    export JFS_NO_USAGE_REPORT="${JFS_NO_USAGE_REPORT:-true}"
    export JFS_CACHE_DIR="${JFS_CACHE_DIR:-/var/lib/juicefs/cache}"
    export PERF_FIO_PREFILL_DRAIN="${PERF_FIO_PREFILL_DRAIN:-true}"
    export PERF_FIO_PREFILL_REMOUNT="${PERF_FIO_PREFILL_REMOUNT:-true}"
    export PERF_FIO_COLD_READ_CLEAR_CACHE="${PERF_FIO_COLD_READ_CLEAR_CACHE:-true}"
    export PERF_FIO_POST_WRITE_DRAIN="${PERF_FIO_POST_WRITE_DRAIN:-true}"
fi

if [[ "${PERF_FIO_COLD_READ:-false}" =~ ^(1|true|yes|on)$ \
    || "${PERF_FIO_COLD_READ_CLEAR_CACHE:-false}" =~ ^(1|true|yes|on)$ ]]; then
    export JFS_CACHE_DIR="${JFS_CACHE_DIR:-/var/lib/juicefs/cache}"
fi

mkdir -p "$ARTIFACTS_DIR"

preclean_ports() {
    local -a ports=(16379 19000 19001)
    for port in "${ports[@]}"; do
        local pid
        pid=$(ss -tlnp 2>/dev/null | awk -v p=":${port}\$" '$0 ~ p {sub(/.*pid=/, ""); sub(/,.*/, ""); print $0}') || true
        if [[ -n "$pid" ]]; then
            local pname
            pname=$(ps -p "$pid" -o comm= 2>/dev/null || echo "unknown")
            if [[ "$pname" == "docker-proxy" ]]; then
                info "端口 $port 被 docker-proxy (pid=$pid) 占用，尝试停止关联容器"
                local cid
                cid=$(docker ps -q --filter "publish=$port" 2>/dev/null) || true
                if [[ -n "$cid" ]]; then
                    docker stop "$cid" 2>/dev/null || true
                    docker rm -f "$cid" 2>/dev/null || true
                fi
            else
                err "端口 $port 被进程 $pname (pid=$pid) 占用，请手动释放"
            fi
        fi
    done
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
}
preclean_ports

cleanup() {
    if [[ "$KEEP" == true ]]; then
        info "跳过 compose down (--keep)"
        return 0
    fi
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

write_warning_summary() {
    local log_path="$1"
    local summary_path="$2"

    {
        printf 'pattern\tcount\n'
        for pattern in WARNING timeout 'slow request' 'slow operation'; do
            local regex="$pattern"
            if [[ "$pattern" == "timeout" ]]; then
                regex='(^|[^[:alnum:]_=-])(timeout|timed out|超时)([^[:alnum:]_=-]|$)'
            fi
            local count
            count=$(awk -v pat="$regex" 'BEGIN { IGNORECASE = 1 } $0 ~ pat { c++ } END { print c + 0 }' "$log_path")
            printf '%s\t%s\n' "$pattern" "$count"
        done
    } >"$summary_path"
}

info "构建 JuiceFS perf 镜像"
docker compose -f "$COMPOSE_FILE" build perf

ts="$(date +%s)-$RANDOM"
host_artifact_dir="$ARTIFACTS_DIR/juicefs-perf-run-${ts}"
mkdir -p "$host_artifact_dir"
runner_console_log="$host_artifact_dir/runner-console.log"
runner_warning_summary="$host_artifact_dir/runner-warning-summary.tsv"
: >"$runner_console_log"

export BREWFS_ARTIFACT_DIR="/artifacts/juicefs-perf-run-${ts}"
export BREWFS_S3_BUCKET="${BREWFS_S3_BUCKET:-brewfs-data}"

services=(redis rustfs)
info "启动依赖服务: ${services[*]}"
docker compose -f "$COMPOSE_FILE" up -d "${services[@]}"

info "初始化 rustfs bucket（一次性容器）"
docker compose -f "$COMPOSE_FILE" run --rm rustfs-init

info "运行容器内性能测试（退出码由 perf 容器决定）"
set +e
docker compose -f "$COMPOSE_FILE" run --rm --no-deps \
    -e PERF_TOOLS="$PERF_TOOLS_VALUE" \
    -e PERF_DIRSTRESS_ARGS \
    -e PERF_DIRPERF_ARGS \
    -e PERF_METAPERF_ARGS \
    -e PERF_LOOPTEST_ARGS \
    -e PERF_DIRSTRESS_PROCS \
    -e PERF_DIRSTRESS_FILES \
    -e PERF_DIRSTRESS_PROCS_PER_DIR \
    -e PERF_METAPERF_SECONDS \
    -e PERF_METAPERF_FILE_SIZE \
    -e PERF_METAPERF_OP_FILES \
    -e PERF_METAPERF_BG_FILES \
    -e PERF_LOOPTEST_ITERS \
    -e PERF_LOOPTEST_BUF_SIZE \
    -e PERF_STRESS_NG_ARGS \
    -e PERF_STRESS_NG_TIMEOUT \
    -e PERF_STRESS_NG_DIR_WORKERS \
    -e PERF_STRESS_NG_DIR_OPS \
    -e PERF_STRESS_NG_DENTRY_WORKERS \
    -e PERF_STRESS_NG_DENTRY_OPS \
    -e PERF_STRESS_NG_RENAME_WORKERS \
    -e PERF_STRESS_NG_RENAME_OPS \
    -e PERF_STRESS_NG_UNLINK_WORKERS \
    -e PERF_STRESS_NG_UNLINK_OPS \
    -e PERF_STRESS_NG_HDD_WORKERS \
    -e PERF_STRESS_NG_HDD_BYTES \
    -e PERF_STRESS_NG_HDD_WRITE_SIZE \
    -e PERF_FIO_ARGS \
    -e PERF_FIO_SEQREAD_ARGS \
    -e PERF_FIO_SEQREAD_BS \
    -e PERF_FIO_SEQREAD_SIZE \
    -e PERF_FIO_SEQREAD_NUMJOBS \
    -e PERF_FIO_SEQREAD_IOENGINE \
    -e PERF_FIO_SEQREAD_IODEPTH \
    -e PERF_FIO_SEQREAD_DIRECT \
    -e PERF_FIO_SEQREAD_DIRECT_MATRIX \
    -e PERF_FIO_SEQREAD_RUNTIME \
    -e PERF_FIO_SEQWRITE_ARGS \
    -e PERF_FIO_SEQWRITE_BS \
    -e PERF_FIO_SEQWRITE_SIZE \
    -e PERF_FIO_SEQWRITE_NUMJOBS \
    -e PERF_FIO_SEQWRITE_IOENGINE \
    -e PERF_FIO_SEQWRITE_IODEPTH \
    -e PERF_FIO_SEQWRITE_DIRECT \
    -e PERF_FIO_SEQWRITE_DIRECT_MATRIX \
    -e PERF_FIO_SEQWRITE_RUNTIME \
    -e PERF_FIO_RANDREAD_ARGS \
    -e PERF_FIO_RANDREAD_BS \
    -e PERF_FIO_RANDREAD_SIZE \
    -e PERF_FIO_RANDREAD_NUMJOBS \
    -e PERF_FIO_RANDREAD_IOENGINE \
    -e PERF_FIO_RANDREAD_IODEPTH \
    -e PERF_FIO_RANDREAD_DIRECT \
    -e PERF_FIO_RANDREAD_DIRECT_MATRIX \
    -e PERF_FIO_RANDREAD_RUNTIME \
    -e PERF_FIO_RANDWRITE_ARGS \
    -e PERF_FIO_RANDWRITE_BS \
    -e PERF_FIO_RANDWRITE_SIZE \
    -e PERF_FIO_RANDWRITE_NUMJOBS \
    -e PERF_FIO_RANDWRITE_IOENGINE \
    -e PERF_FIO_RANDWRITE_IODEPTH \
    -e PERF_FIO_RANDWRITE_DIRECT \
    -e PERF_FIO_RANDWRITE_DIRECT_MATRIX \
    -e PERF_FIO_RANDWRITE_RUNTIME \
    -e PERF_FIO_RANDRW_ARGS \
    -e PERF_FIO_RANDRW_BS \
    -e PERF_FIO_RANDRW_SIZE \
    -e PERF_FIO_RANDRW_NUMJOBS \
    -e PERF_FIO_RANDRW_IOENGINE \
    -e PERF_FIO_RANDRW_IODEPTH \
    -e PERF_FIO_RANDRW_DIRECT \
    -e PERF_FIO_RANDRW_DIRECT_MATRIX \
    -e PERF_FIO_RANDRW_RUNTIME \
    -e PERF_FIO_BIGREAD_ARGS \
    -e PERF_FIO_BIGREAD_BS \
    -e PERF_FIO_BIGREAD_SIZE \
    -e PERF_FIO_BIGREAD_NUMJOBS \
    -e PERF_FIO_BIGREAD_IOENGINE \
    -e PERF_FIO_BIGREAD_IODEPTH \
    -e PERF_FIO_BIGREAD_DIRECT \
    -e PERF_FIO_BIGREAD_DIRECT_MATRIX \
    -e PERF_FIO_BIGWRITE_ARGS \
    -e PERF_FIO_BIGWRITE_BS \
    -e PERF_FIO_BIGWRITE_SIZE \
    -e PERF_FIO_BIGWRITE_NUMJOBS \
    -e PERF_FIO_BIGWRITE_IOENGINE \
    -e PERF_FIO_BIGWRITE_IODEPTH \
    -e PERF_FIO_BIGWRITE_DIRECT \
    -e PERF_FIO_BIGWRITE_DIRECT_MATRIX \
    -e PERF_FIO_NAME \
    -e PERF_FIO_RW \
    -e PERF_FIO_RWMIXREAD \
    -e PERF_FIO_BS \
    -e PERF_FIO_SIZE \
    -e PERF_FIO_NUMJOBS \
    -e PERF_FIO_IOENGINE \
    -e PERF_FIO_IODEPTH \
    -e PERF_FIO_DIRECT \
    -e PERF_FIO_DIRECT_MATRIX \
    -e PERF_FIO_RUNTIME \
    -e PERF_FIO_COLD_READ \
    -e PERF_FIO_PREFILL_DRAIN \
    -e PERF_FIO_PREFILL_REMOUNT \
    -e PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS \
    -e PERF_FIO_PREFILL_DRAIN_INTERVAL_SECS \
    -e PERF_FIO_PREFILL_DRAIN_PENDING_BYTES \
    -e PERF_FIO_POST_WRITE_DRAIN \
    -e PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS \
    -e PERF_FIO_POST_WRITE_DRAIN_INTERVAL_SECS \
    -e PERF_FIO_POST_WRITE_DRAIN_PENDING_BYTES \
    -e PERF_FIO_DROP_CACHES \
    -e PERF_FIO_COLD_READ_DROP_CACHES \
    -e PERF_FIO_COLD_READ_CLEAR_CACHE \
    -e JFS_COMPRESS \
    -e JFS_WRITEBACK \
    -e JFS_BUFFER_SIZE_MIB \
    -e JFS_CACHE_SIZE_MIB \
    -e JFS_CACHE_LARGE_WRITE \
    -e JFS_MAX_UPLOADS \
    -e JFS_MAX_DOWNLOADS \
    -e JFS_MAX_READAHEAD_MIB \
    -e JFS_PREFETCH \
    -e JFS_OPEN_CACHE \
    -e JFS_OPEN_CACHE_LIMIT \
    -e JFS_BACKUP_META \
    -e JFS_NO_USAGE_REPORT \
    -e JFS_CACHE_DIR \
    -e PERF_LOG_TO_CONSOLE \
    perf 2>&1 | tee "$runner_console_log"
container_status=${PIPESTATUS[0]}
set -e
write_warning_summary "$runner_console_log" "$runner_warning_summary"

ok "JuiceFS perf compose 运行结束 (container=$container_status)"
ok "产物目录: $host_artifact_dir"
exit "$container_status"
