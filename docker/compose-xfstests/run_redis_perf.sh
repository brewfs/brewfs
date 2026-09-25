#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(realpath "$SCRIPT_DIR/..")"
PROJECT_DIR="$(realpath "$DOCKER_DIR/..")"

COMPOSE_FILE="$SCRIPT_DIR/docker-compose.redis-perf.yml"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

usage() {
    cat <<EOF
用法: $(basename "$0") [选项]

说明:
  - 使用 docker compose 在容器内运行 xfstests 压力工具，元数据库为 redis
  - 默认使用 rustfs 作为对象存储后端
  - 可选附带运行宿主机上的 brewfs_bench
  - 测试产物输出到: $ARTIFACTS_DIR/perf-run-*

选项:
  --s3                       使用 rustfs 作为对象存储（默认）
  --minio                    使用 MinIO 作为对象存储
  --local-fs                 改为使用本地目录作为对象存储
  --s3-writeback             启用 S3 commit-before-upload 写回语义（等价于 BREWFS_WRITEBACK_MODE=commit_before_upload）
  --read-throughput-profile  启用读吞吐 profile：只读句柄返回 FOPEN_DIRECT_IO，减少 buffered FUSE 大读拆分（会禁用这些只读 fd 的 mmap）
  --metadata-throughput-profile
                             启用 single-client metadata profile：允许 non-append 写句柄复用 1s open attr cache（会弱化跨客户端 close-to-open freshness）
  --bigwrite-throughput-profile
                             启用 fio-bigwrite/大 buffered write 吞吐 profile；这是 --writeback-throughput-profile 的明确别名，会启用 commit-before-upload 写回语义
  --writeback-throughput-profile
                             启用 S3 writeback 全场景吞吐 profile（cache root=/var/lib/brewfs/cache, 4GiB memory + 8GiB read-SSD/4GiB write-SSD cache, 上传后持久块缓存并在 prefill remount 后保留, 12GiB memory budget, S3 max concurrency=16, writeback upload concurrency=6, pending soft/hard=2GiB/3GiB, writeback persist fsync=false, compression=none, full cache checksum, fuse workers=16, fuse max_background=512, read-throughput profile, fio prefill/post-write drain+remount）
  --tools "<tool...>"        指定压力工具列表，默认: "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"
  --brewfs-bench           额外运行一次宿主机 cargo bench --bench brewfs_bench
  --bench-args "<args...>"   透传给 cargo bench 之后的 Criterion 参数
  --keep                     结束后不执行 compose down（便于调试）
  -h, --help                 显示帮助

支持的 PERF_TOOLS:
  fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw fio dirstress dirperf metaperf looptest object-put-bench

可通过环境变量覆盖各工具参数:
  PERF_DIRSTRESS_ARGS PERF_DIRPERF_ARGS PERF_METAPERF_ARGS PERF_LOOPTEST_ARGS
  PERF_STRESS_NG_ARGS 可完全覆盖默认 stress-ng 参数；如需 link/symlink stressor 请用该变量显式指定
  PERF_METADATA_POST_TOOL_DRAIN PERF_METADATA_POST_TOOL_DRAIN_TIMEOUT_SECS PERF_METADATA_POST_TOOL_DRAIN_PENDING_BYTES
  PERF_FIO_ARGS PERF_FIO_RUNTIME PERF_FIO_SIZE PERF_FIO_BS PERF_FIO_NUMJOBS PERF_FIO_DIRECT
  PERF_FIO_BIGREAD_REPEATS=1|3|5 PERF_FIO_BIGREAD_WARMUP_PASSES=0|1 PERF_FIO_BIGREAD_COOLDOWN_SECS=10 PERF_FIO_BIGREAD_EVICT_LOCAL_CACHE_PAGES=true PERF_FIO_BIGREAD_REMOUNT_BETWEEN_REPEATS=true
  PERF_FIO_DIRECT_MATRIX="0 1" 可对 fio profile 显式跑 buffered/direct 矩阵（默认不启用）
  PERF_FIO_{SEQREAD,SEQWRITE,RANDREAD,RANDWRITE,RANDRW,BIGREAD,BIGWRITE}_{ARGS,BS,SIZE,NUMJOBS,IOENGINE,IODEPTH,DIRECT,DIRECT_MATRIX,RUNTIME}
  PERF_FIO_COLD_READ PERF_FIO_PREFILL_DRAIN PERF_FIO_PREFILL_REMOUNT PERF_FIO_PREFILL_DRAIN_TIMEOUT_SECS PERF_FIO_PREFILL_DRAIN_PENDING_BYTES
  PERF_FIO_POST_WRITE_DRAIN PERF_FIO_POST_WRITE_DRAIN_TIMEOUT_SECS PERF_FIO_POST_WRITE_DRAIN_PENDING_BYTES
  PERF_FIO_COLD_READ_CLEAR_CACHE PERF_FIO_DROP_CACHES
  PERF_OBJECT_PUT_DURATION_SECS PERF_OBJECT_PUT_OBJECTS PERF_OBJECT_PUT_OBJECT_SIZE PERF_OBJECT_PUT_WORKERS PERF_OBJECT_PUT_PREFIX
  BREWFS_CHUNK_SIZE BREWFS_BLOCK_SIZE
  BREWFS_S3_PART_SIZE BREWFS_S3_MAX_CONCURRENCY BREWFS_S3_DISABLE_PAYLOAD_CHECKSUM
  BREWFS_READ_MEMORY_BYTES BREWFS_READ_SSD_BYTES BREWFS_WRITE_MEMORY_BYTES BREWFS_WRITE_SSD_BYTES
  BREWFS_DIRTY_SLICE_TARGET_SIZE BREWFS_DIRTY_SLICE_MAX_AGE_MS BREWFS_UPLOAD_CONCURRENCY
  BREWFS_PREFETCH_ENABLED BREWFS_PREFETCH_MAX_BYTES BREWFS_PREFETCH_CONCURRENCY BREWFS_RANGE_BACKGROUND_PREFETCH BREWFS_MEMORY_BUDGET_BYTES
  BREWFS_FUSE_WORKERS BREWFS_FUSE_MAX_BACKGROUND BREWFS_FUSE_DIRECT_IO BREWFS_FUSE_READ_DIRECT_IO BREWFS_FUSE_WRITE_DIRECT_IO BREWFS_FUSE_KEEP_CACHE BREWFS_FUSE_WRITEBACK
  BREWFS_WRITEBACK_UPLOAD_CONCURRENCY
  BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES
  BREWFS_WRITEBACK_PERSIST_SYNC BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT
  BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD=false 可关闭上传后最近写内存读缓存
  BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD=true 可把上传后写缓存同时持久化到本地磁盘读缓存
  BREWFS_CACHED_BLOCK_ASSEMBLER BREWFS_CACHED_SUB_BLOCK_IDLE_GRACE_MS BREWFS_CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE_MS
  BREWFS_VERIFY_CACHE_CHECKSUM
  BREWFS_UPLOAD_LIMIT_MIBPS BREWFS_DOWNLOAD_LIMIT_MIBPS
  BREWFS_METADATA_OPEN_CACHE_TTL_MS BREWFS_METADATA_OPEN_CACHE_CAPACITY BREWFS_METADATA_ALLOW_WRITE_OPEN_CACHE
  BREWFS_COMPACT_INTERVAL_SECS BREWFS_COMPACT_MIN_SLICE_COUNT BREWFS_COMPACT_ASYNC_THRESHOLD BREWFS_COMPACT_SYNC_THRESHOLD BREWFS_COMPACT_MAX_CHUNKS_PER_RUN
  BREWFS_WRITEBACK_MODE=commit_before_upload 可启用 S3 写回语义
  REDIS_PERF_DATA_MOUNT 可把 Redis AOF/RDB 数据挂到大容量目录或命名卷（例如 /data/slayer/brewfs-perf-redis）
  PERF_FIO_COLD_READ=true 可在读类 fio 预填充后等待写回 drain、清理 BrewFS 本地 cache root 并重挂载，再执行读测试
  PERF_FIO_DROP_CACHES=true 可在 cold-read 重挂载前尝试 drop_caches（失败时继续）
  BREWFS_FUSE_DIRECT_IO=1 可对所有句柄启用 direct_io（会禁用 mmap）
  BREWFS_FUSE_READ_DIRECT_IO=1 可只对只读句柄启用 direct_io（会禁用 mmap）
  BREWFS_FUSE_WRITE_DIRECT_IO=1 可只对只写句柄启用 direct_io（会禁用 mmap）
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

truthy_env() {
    local value
    value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
    case "$value" in
        1|true|yes|on) return 0 ;;
        *) return 1 ;;
    esac
}

size_to_bytes() {
    local value="${1:-}"
    local number suffix multiplier
    if [[ "$value" =~ ^[0-9]+$ ]]; then
        printf '%s' "$value"
        return 0
    fi
    if [[ ! "$value" =~ ^([0-9]+)([kKmMgGtTpP])([iI]?[bB])?$ ]]; then
        return 1
    fi
    number="${BASH_REMATCH[1]}"
    suffix="${BASH_REMATCH[2]}"
    case "${suffix,,}" in
        k) multiplier=1024 ;;
        m) multiplier=$((1024 * 1024)) ;;
        g) multiplier=$((1024 * 1024 * 1024)) ;;
        t) multiplier=$((1024 * 1024 * 1024 * 1024)) ;;
        p) multiplier=$((1024 * 1024 * 1024 * 1024 * 1024)) ;;
        *) return 1 ;;
    esac
    if (( number > (9223372036854775807 / multiplier) )); then
        return 1
    fi
    printf '%s' "$((number * multiplier))"
}

KEEP=false
STORAGE_BACKEND="rustfs"  # rustfs | minio | local-fs
RUN_BREWFS_BENCH=false
READ_THROUGHPUT_PROFILE=false
METADATA_THROUGHPUT_PROFILE=false
WRITEBACK_THROUGHPUT_PROFILE=false
PERF_TOOLS_VALUE="fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"
BENCH_ARGS_VALUE=""
BREWFS_WRITEBACK_MODE_VALUE="${BREWFS_WRITEBACK_MODE:-}"

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --s3)
            STORAGE_BACKEND="rustfs"
            shift
            ;;
        --minio)
            STORAGE_BACKEND="minio"
            shift
            ;;
        --local-fs)
            STORAGE_BACKEND="local-fs"
            shift
            ;;
        --s3-writeback)
            BREWFS_WRITEBACK_MODE_VALUE="commit_before_upload"
            shift
            ;;
        --read-throughput-profile)
            READ_THROUGHPUT_PROFILE=true
            shift
            ;;
        --metadata-throughput-profile)
            METADATA_THROUGHPUT_PROFILE=true
            shift
            ;;
        --bigwrite-throughput-profile)
            READ_THROUGHPUT_PROFILE=true
            WRITEBACK_THROUGHPUT_PROFILE=true
            BREWFS_WRITEBACK_MODE_VALUE="commit_before_upload"
            shift
            ;;
        --writeback-throughput-profile)
            READ_THROUGHPUT_PROFILE=true
            WRITEBACK_THROUGHPUT_PROFILE=true
            BREWFS_WRITEBACK_MODE_VALUE="commit_before_upload"
            shift
            ;;
        --tools)
            require_value "$1" "${2:-}"
            PERF_TOOLS_VALUE="${2:-}"
            shift 2
            ;;
        --brewfs-bench)
            RUN_BREWFS_BENCH=true
            shift
            ;;
        --bench-args)
            require_value "$1" "${2:-}"
            BENCH_ARGS_VALUE="${2:-}"
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

if [[ -n "$BREWFS_WRITEBACK_MODE_VALUE" && "$STORAGE_BACKEND" == "local-fs" ]]; then
    err "S3 writeback mode requires --s3 or --minio, not --local-fs"
    exit 1
fi
export BREWFS_WRITEBACK_MODE="$BREWFS_WRITEBACK_MODE_VALUE"

if [[ "$READ_THROUGHPUT_PROFILE" == true ]]; then
    # Keep the default mount mmap-friendly. The throughput profile is explicit
    # because FOPEN_DIRECT_IO disables mmap on the affected read-only handles.
    export BREWFS_FUSE_READ_DIRECT_IO="${BREWFS_FUSE_READ_DIRECT_IO:-1}"
fi

if [[ "$METADATA_THROUGHPUT_PROFILE" == true ]]; then
    export BREWFS_METADATA_OPEN_CACHE_TTL_MS="${BREWFS_METADATA_OPEN_CACHE_TTL_MS:-1000}"
    export BREWFS_METADATA_OPEN_CACHE_CAPACITY="${BREWFS_METADATA_OPEN_CACHE_CAPACITY:-65536}"
    export BREWFS_METADATA_ALLOW_WRITE_OPEN_CACHE="${BREWFS_METADATA_ALLOW_WRITE_OPEN_CACHE:-true}"
fi

enable_writeback_throughput_profile() {
    # Opt-in profile for evidence-backed large-write benchmarks. It favors
    # local writeback throughput over upload-before-commit durability semantics.
    export BREWFS_CACHE_ROOT="${BREWFS_CACHE_ROOT:-/var/lib/brewfs/cache}"
    export BREWFS_READ_MEMORY_BYTES="${BREWFS_READ_MEMORY_BYTES:-4294967296}"
    export BREWFS_WRITE_MEMORY_BYTES="${BREWFS_WRITE_MEMORY_BYTES:-4294967296}"
    # Allow a 4GiB fio prefill plus integrity framing to survive remount intact.
    export BREWFS_READ_SSD_BYTES="${BREWFS_READ_SSD_BYTES:-8589934592}"
    export BREWFS_WRITE_SSD_BYTES="${BREWFS_WRITE_SSD_BYTES:-4294967296}"
    export BREWFS_MEMORY_BUDGET_BYTES="${BREWFS_MEMORY_BUDGET_BYTES:-12884901888}"
    export BREWFS_S3_MAX_CONCURRENCY="${BREWFS_S3_MAX_CONCURRENCY:-16}"
    export BREWFS_WRITEBACK_UPLOAD_CONCURRENCY="${BREWFS_WRITEBACK_UPLOAD_CONCURRENCY:-6}"
    export BREWFS_UPLOAD_CONCURRENCY="${BREWFS_UPLOAD_CONCURRENCY:-32}"
    export BREWFS_DIRTY_SLICE_TARGET_SIZE="${BREWFS_DIRTY_SLICE_TARGET_SIZE:-67108864}"
    export BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES="${BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES:-2147483648}"
    export BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES="${BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES:-3221225472}"
    export BREWFS_WRITEBACK_PERSIST_SYNC="${BREWFS_WRITEBACK_PERSIST_SYNC:-false}"
    export BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT="${BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT:-false}"
    export BREWFS_CACHED_BLOCK_ASSEMBLER="${BREWFS_CACHED_BLOCK_ASSEMBLER:-true}"
    export BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD="${BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD:-true}"
    export BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD="${BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD:-true}"
    export BREWFS_COMPRESSION="${BREWFS_COMPRESSION:-none}"
    export BREWFS_VERIFY_CACHE_CHECKSUM="${BREWFS_VERIFY_CACHE_CHECKSUM:-full}"
    # Eight fio jobs need enough request consumers to overlap object GET latency.
    # The 16-worker setting passed the complete data and metadata regression gate.
    export BREWFS_FUSE_WORKERS="${BREWFS_FUSE_WORKERS:-16}"
    export BREWFS_FUSE_MAX_BACKGROUND="${BREWFS_FUSE_MAX_BACKGROUND:-512}"
    export BREWFS_METADATA_OPEN_CACHE_TTL_MS="${BREWFS_METADATA_OPEN_CACHE_TTL_MS:-1000}"
    export BREWFS_METADATA_OPEN_CACHE_CAPACITY="${BREWFS_METADATA_OPEN_CACHE_CAPACITY:-65536}"
    export PERF_FIO_PREFILL_DRAIN="${PERF_FIO_PREFILL_DRAIN:-true}"
    export PERF_FIO_PREFILL_REMOUNT="${PERF_FIO_PREFILL_REMOUNT:-true}"
    # Preserve BrewFS's persistent disk block cache across the prefill remount.
    export PERF_FIO_COLD_READ_CLEAR_CACHE="${PERF_FIO_COLD_READ_CLEAR_CACHE:-false}"
    export PERF_FIO_POST_WRITE_DRAIN="${PERF_FIO_POST_WRITE_DRAIN:-true}"
    export PERF_METADATA_POST_TOOL_DRAIN="${PERF_METADATA_POST_TOOL_DRAIN:-true}"
}

if [[ "$WRITEBACK_THROUGHPUT_PROFILE" == true ]]; then
    enable_writeback_throughput_profile
fi

PACKED_MODE=false
if [[ "${BREWFS_VOLUME_FORMAT:-}" == "packed-metadata-v1" ]]; then
    PACKED_MODE=true
    if [[ "$STORAGE_BACKEND" == "local-fs" ]]; then
        err "packed-metadata-v1 requires --s3 or --minio because its fixture is published to object storage"
        exit 1
    fi
    if [[ "$RUN_BREWFS_BENCH" == true ]]; then
        err "--brewfs-bench is a Redis metadata benchmark and cannot run in packed-metadata-v1 mode"
        exit 1
    fi
    export BREWFS_META_BACKEND="${BREWFS_META_BACKEND:-none}"
    export BREWFS_META_URL="${BREWFS_META_URL:-}"
    export BREWFS_CARGO_BUILD_ARGS="${BREWFS_CARGO_BUILD_ARGS:---features native-packed-base,frozen-base-metadata}"
    if truthy_env "${PERF_FIO_COLD_READ:-false}" \
        || truthy_env "${PERF_FIO_COLD_READ_CLEAR_CACHE:-false}"; then
        # A cold-read profile must not warm and reuse BrewFS's block cache
        # during a time-based fio job. Keep explicit caller budgets intact so
        # cached packed profiles remain available when requested deliberately.
        export BREWFS_READ_MEMORY_BYTES="${BREWFS_READ_MEMORY_BYTES:-0}"
        export BREWFS_READ_SSD_BYTES="${BREWFS_READ_SSD_BYTES:-0}"
        export BREWFS_PREFETCH_ENABLED="${BREWFS_PREFETCH_ENABLED:-false}"
        export BREWFS_RANGE_BACKGROUND_PREFETCH="${BREWFS_RANGE_BACKGROUND_PREFETCH:-false}"
        export BREWFS_CACHE_ROOT="${BREWFS_CACHE_ROOT:-/tmp/brewfs-packed-cold-cache}"
    fi
fi

mkdir -p "$ARTIFACTS_DIR"

# 预清理：杀掉占用目标端口的残留容器，确保 compose up 不会端口冲突
preclean_ports() {
    local -a ports=(16379 19000 19001)
    for port in "${ports[@]}"; do
        local pid
        pid=$(ss -tlnp 2>/dev/null | awk -v p=":${port}\$" '$0 ~ p {sub(/.*pid=/, ""); sub(/,.*/, ""); print $0}') || true
        if [[ -n "$pid" ]]; then
            local pname
            pname=$(ps -p "$pid" -o comm= 2>/dev/null || echo "unknown")
            # 只杀 docker-proxy / containers 相关进程，避免误杀用户自己的服务
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
    # 确保之前的 compose 资源已释放
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

run_brewfs_bench() {
    local host_artifact_dir="$1"
    local bench_artifact_dir="$host_artifact_dir/brewfs-bench"
    local benchmark_meta_url="redis://127.0.0.1:${REDIS_HOST_PORT:-16379}/0"
    local -a bench_args=()

    mkdir -p "$bench_artifact_dir"
    if [[ -n "$BENCH_ARGS_VALUE" ]]; then
        read -r -a bench_args <<<"$BENCH_ARGS_VALUE"
    fi

    info "运行宿主机 brewfs_bench（redis backend）"
    set +e
    (
        cd "$PROJECT_DIR"
        env \
            RUST_LOG="${RUST_LOG:-warn}" \
            BREWFS_BENCH_META_BACKEND=redis \
            BREWFS_BENCH_META_URL="$benchmark_meta_url" \
            BREWFS_BENCH_BACKEND="$([[ "$STORAGE_BACKEND" == "local-fs" ]] && echo local || echo s3)" \
            BREWFS_BENCH_S3_BUCKET="${BREWFS_S3_BUCKET:-brewfs-data}" \
            BREWFS_BENCH_S3_REGION="${BREWFS_S3_REGION:-us-east-1}" \
            BREWFS_BENCH_S3_ENDPOINT="http://127.0.0.1:${S3_HOST_PORT:-19000}" \
            BREWFS_BENCH_S3_FORCE_PATH_STYLE=true \
            AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-${S3_ACCESS_KEY}}" \
            AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-${S3_SECRET_KEY}}" \
            AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
            cargo bench -p brewfs --bench brewfs_bench -- "${bench_args[@]}"
    ) 2>&1 | tee "$bench_artifact_dir/console.log"
    local bench_status="${PIPESTATUS[0]}"
    set -e

    if [[ -d "$PROJECT_DIR/target/criterion" ]]; then
        rm -rf "$bench_artifact_dir/criterion"
        cp -a "$PROJECT_DIR/target/criterion" "$bench_artifact_dir/criterion" || true
    fi

    return "$bench_status"
}

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

info "构建宿主机 brewfs release 二进制（供镜像 COPY）"
bash "$DOCKER_DIR/build_brewfs_host_binary.sh"

info "构建 perf runner 镜像"
docker compose -f "$COMPOSE_FILE" build perf

ts="$(date +%s)-$RANDOM"
host_artifact_dir="$ARTIFACTS_DIR/perf-run-${ts}"
mkdir -p "$host_artifact_dir"
runner_console_log="$host_artifact_dir/runner-console.log"
runner_warning_summary="$host_artifact_dir/runner-warning-summary.tsv"
: >"$runner_console_log"

export BREWFS_ARTIFACT_DIR="/artifacts/perf-run-${ts}"
export BREWFS_S3_BUCKET="${BREWFS_S3_BUCKET:-brewfs-data}"

# 根据存储后端设置 S3 相关变量
case "$STORAGE_BACKEND" in
    rustfs)
        export BREWFS_DATA_BACKEND="s3"
        export BREWFS_S3_ENDPOINT="${BREWFS_S3_ENDPOINT:-http://rustfs:9000}"
        export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-rustfsadmin}"
        export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-rustfsadmin}"
        S3_ACCESS_KEY="${AWS_ACCESS_KEY_ID}"
        S3_SECRET_KEY="${AWS_SECRET_ACCESS_KEY}"
        S3_HOST_PORT="${RUSTFS_S3_HOST_PORT:-19000}"
        storage_service="rustfs"
        init_service="rustfs-init"
        ;;
    minio)
        export BREWFS_DATA_BACKEND="s3"
        export BREWFS_S3_ENDPOINT="${BREWFS_S3_ENDPOINT:-http://minio:9000}"
        export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
        export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
        S3_ACCESS_KEY="${AWS_ACCESS_KEY_ID}"
        S3_SECRET_KEY="${AWS_SECRET_ACCESS_KEY}"
        S3_HOST_PORT="${MINIO_S3_HOST_PORT:-19000}"
        storage_service="minio"
        init_service="minio-init"
        ;;
    local-fs)
        export BREWFS_DATA_BACKEND="local-fs"
        S3_ACCESS_KEY=""
        S3_SECRET_KEY=""
        S3_HOST_PORT=""
        storage_service=""
        init_service=""
        ;;
esac

publish_packed_fixture() {
    local fixture_log="$host_artifact_dir/packed-fixture.log"
    local manifest_output="$host_artifact_dir/packed-manifest-key.txt"
    local fixture_bin="$PROJECT_DIR/target/release/packed_snapshot_fixture"
    if [[ -n "${BREWFS_PACKED_MANIFEST_KEY:-}" ]]; then
        printf '%s\n' "$BREWFS_PACKED_MANIFEST_KEY" >"$manifest_output"
        info "复用 packed manifest key: $BREWFS_PACKED_MANIFEST_KEY"
        return 0
    fi

    info "构建并发布 packed metadata fixture（无 Redis）"
    cargo build --release \
        --features native-packed-base,frozen-base-metadata \
        --bin packed_snapshot_fixture
    if [[ ! -x "$fixture_bin" ]]; then
        err "fixture binary not found: $fixture_bin"
        return 1
    fi
    local prefix="${BREWFS_PACKED_FIXTURE_PREFIX:-packed-fixture-${ts}}"
    local fio_file_size="${PERF_PACKED_FIO_FILE_SIZE:-67108864}"
    if ! fio_file_size="$(size_to_bytes "$fio_file_size")"; then
        err "PERF_PACKED_FIO_FILE_SIZE 不是有效容量: ${PERF_PACKED_FIO_FILE_SIZE:-<empty>}"
        return 2
    fi
    # The in-container scanner and POSIX checks use this value as an integer;
    # normalize human-friendly input once before publishing and passing env on.
    export PERF_PACKED_FIO_FILE_SIZE="$fio_file_size"
    set +e
    env \
        AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
        AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
        AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
        "$fixture_bin" \
        --bucket "$BREWFS_S3_BUCKET" \
        --endpoint "http://127.0.0.1:${S3_HOST_PORT}" \
        --region "${BREWFS_S3_REGION:-us-east-1}" \
        --prefix "$prefix" \
        --dirs "${PERF_PACKED_DIRS:-8}" \
        --dir-levels "${PERF_PACKED_DIR_LEVELS:-0}" \
        --dirs-per-level "${PERF_PACKED_DIRS_PER_LEVEL:-10}" \
        --files-per-dir "${PERF_PACKED_FILES_PER_DIR:-4500}" \
        --small-file-size "${PERF_PACKED_SMALLFILE_SIZE:-4096}" \
        --fio-file-size "$fio_file_size" \
        --manifest-output "$manifest_output" \
        >"$fixture_log" 2>&1
    local fixture_status=$?
    set -e
    if [[ "$fixture_status" -ne 0 ]]; then
        err "packed fixture 发布失败 (exit=$fixture_status)，日志: $fixture_log"
        return "$fixture_status"
    fi
    BREWFS_PACKED_MANIFEST_KEY="$(tr -d '\r\n' <"$manifest_output")"
    if [[ -z "$BREWFS_PACKED_MANIFEST_KEY" ]]; then
        err "packed fixture 未产生 manifest key"
        return 1
    fi
    export BREWFS_PACKED_MANIFEST_KEY
    printf 'manifest_key=%s\n' "$BREWFS_PACKED_MANIFEST_KEY" >>"$fixture_log"
    ok "packed fixture 已发布: $BREWFS_PACKED_MANIFEST_KEY"
}

services=()
if [[ "$PACKED_MODE" != true ]]; then
    services+=(redis)
fi
if [[ -n "$storage_service" ]]; then
    services+=("$storage_service")
fi
info "启动依赖服务: ${services[*]}"
docker compose -f "$COMPOSE_FILE" up -d "${services[@]}"
docker compose -f "$COMPOSE_FILE" ps >"$host_artifact_dir/compose-services-before-perf.txt" 2>&1 || true
if [[ "$PACKED_MODE" == true ]] && grep -q 'redis-brewfs-perf' "$host_artifact_dir/compose-services-before-perf.txt"; then
    err "packed-metadata-v1 unexpectedly started Redis; refusing to run"
    exit 1
fi

if [[ -n "$init_service" ]]; then
    info "初始化 ${storage_service} bucket（一次性容器）"
    docker compose -f "$COMPOSE_FILE" run --rm "$init_service"
fi

if [[ "$PACKED_MODE" == true ]]; then
    publish_packed_fixture
fi

info "运行容器内性能测试（退出码由 perf 容器决定）"
set +e
docker compose -f "$COMPOSE_FILE" run --rm --no-deps \
    -e PERF_TOOLS="$PERF_TOOLS_VALUE" \
    -e BREWFS_META_BACKEND \
    -e BREWFS_META_URL \
    -e BREWFS_PACKED_MANIFEST_KEY \
    -e PERF_PACKED_DIRS \
    -e PERF_PACKED_DIR_LEVELS \
    -e PERF_PACKED_DIRS_PER_LEVEL \
    -e PERF_PACKED_FILES_PER_DIR \
    -e PERF_PACKED_SMALLFILE_COUNT \
    -e PERF_PACKED_SMALLFILE_SIZE \
    -e PERF_PACKED_SMALLFILE_READ_BYTES \
    -e PERF_PACKED_FIO_FILE_SIZE \
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
    -e PERF_FIO_BIGREAD_REPEATS \
    -e PERF_FIO_BIGREAD_WARMUP_PASSES \
    -e PERF_FIO_BIGREAD_COOLDOWN_SECS \
    -e PERF_FIO_BIGREAD_EVICT_LOCAL_CACHE_PAGES \
    -e PERF_FIO_BIGREAD_REMOUNT_BETWEEN_REPEATS \
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
    -e PERF_METADATA_POST_TOOL_DRAIN \
    -e PERF_METADATA_POST_TOOL_DRAIN_TIMEOUT_SECS \
    -e PERF_METADATA_POST_TOOL_DRAIN_INTERVAL_SECS \
    -e PERF_METADATA_POST_TOOL_DRAIN_PENDING_BYTES \
    -e PERF_FIO_DROP_CACHES \
    -e PERF_FIO_COLD_READ_DROP_CACHES \
    -e PERF_FIO_COLD_READ_CLEAR_CACHE \
    -e PERF_OBJECT_PUT_DURATION_SECS \
    -e PERF_OBJECT_PUT_OBJECTS \
    -e PERF_OBJECT_PUT_OBJECT_SIZE \
    -e PERF_OBJECT_PUT_WORKERS \
    -e PERF_OBJECT_PUT_PREFIX \
    -e PERF_FUSE_OPS_LOG \
    -e BREWFS_FUSE_OP_LOG \
    -e BREWFS_FUSE_DIRECT_IO \
    -e BREWFS_FUSE_WORKERS \
    -e BREWFS_FUSE_MAX_BACKGROUND \
    -e BREWFS_FUSE_READ_DIRECT_IO \
    -e BREWFS_FUSE_WRITE_DIRECT_IO \
    -e BREWFS_FUSE_KEEP_CACHE \
    -e BREWFS_FUSE_WRITEBACK \
    -e BREWFS_NOFILE_LIMIT \
    -e BREWFS_CHUNK_SIZE \
    -e BREWFS_BLOCK_SIZE \
    -e BREWFS_S3_PART_SIZE \
    -e BREWFS_S3_MAX_CONCURRENCY \
    -e BREWFS_S3_DISABLE_PAYLOAD_CHECKSUM \
    -e BREWFS_CACHE_ROOT \
    -e BREWFS_READ_MEMORY_BYTES \
    -e BREWFS_READ_SSD_BYTES \
    -e BREWFS_WRITE_MEMORY_BYTES \
    -e BREWFS_WRITE_SSD_BYTES \
    -e BREWFS_DIRTY_SLICE_TARGET_SIZE \
    -e BREWFS_DIRTY_SLICE_MAX_AGE_MS \
    -e BREWFS_UPLOAD_CONCURRENCY \
    -e BREWFS_PREFETCH_ENABLED \
    -e BREWFS_PREFETCH_MAX_BYTES \
    -e BREWFS_PREFETCH_CONCURRENCY \
    -e BREWFS_RANGE_BACKGROUND_PREFETCH \
    -e BREWFS_MEMORY_BUDGET_BYTES \
    -e BREWFS_COMPRESSION \
    -e BREWFS_WRITEBACK_UPLOAD_CONCURRENCY \
    -e BREWFS_WRITEBACK_RECENT_PENDING_SOFT_BYTES \
    -e BREWFS_WRITEBACK_RECENT_PENDING_HARD_BYTES \
    -e BREWFS_CACHED_BLOCK_ASSEMBLER \
    -e BREWFS_WRITEBACK_PERSIST_SYNC \
    -e BREWFS_WRITEBACK_REQUIRE_STAGE_BEFORE_COMMIT \
    -e BREWFS_POPULATE_WRITE_CACHE_AFTER_UPLOAD \
    -e BREWFS_PERSIST_WRITE_CACHE_AFTER_UPLOAD \
    -e BREWFS_CACHED_SUB_BLOCK_IDLE_GRACE_MS \
    -e BREWFS_CACHED_SUB_BLOCK_TOO_MANY_MIN_AGE_MS \
    -e BREWFS_VERIFY_CACHE_CHECKSUM \
    -e BREWFS_UPLOAD_LIMIT_MIBPS \
    -e BREWFS_DOWNLOAD_LIMIT_MIBPS \
    -e BREWFS_METADATA_OPEN_CACHE_TTL_MS \
    -e BREWFS_METADATA_OPEN_CACHE_CAPACITY \
    -e BREWFS_METADATA_ALLOW_WRITE_OPEN_CACHE \
    -e BREWFS_COMPACT_INTERVAL_SECS \
    -e BREWFS_COMPACT_MIN_SLICE_COUNT \
    -e BREWFS_COMPACT_MIN_FRAGMENT_RATIO \
    -e BREWFS_COMPACT_ASYNC_THRESHOLD \
    -e BREWFS_COMPACT_SYNC_THRESHOLD \
    -e BREWFS_COMPACT_MAX_CHUNKS_PER_RUN \
    -e BREWFS_COMPACT_MAX_CONCURRENT_TASKS \
    -e BREWFS_COMPACT_LIGHT_ENABLED \
    -e BREWFS_COMPACT_LIGHT_THRESHOLD \
    -e BREWFS_COMPACT_HEAVY_ENABLED \
    -e BREWFS_COMPACT_HEAVY_FRAGMENT_THRESHOLD \
    -e BREWFS_COMPACT_HEAVY_SLICE_THRESHOLD \
    -e BREWFS_COMPACT_HEAVY_FORCE_FRAGMENT_THRESHOLD \
    -e BREWFS_COMPACT_LOCK_ASYNC_TTL_SECS \
    -e BREWFS_COMPACT_LOCK_SYNC_TTL_SECS \
    -e BREWFS_COMPACT_LOCK_TTL_PER_SLICE_MS \
    -e BREWFS_COMPACT_LOCK_MIN_TTL_SECS \
    -e BREWFS_COMPACT_LOCK_MAX_TTL_SECS \
    -e BREWFS_WRITEBACK_MODE \
    -e BREWFS_VFS_TIMING \
    -e PERF_LOG_TO_CONSOLE \
    perf 2>&1 | tee "$runner_console_log"
container_status=${PIPESTATUS[0]}
set -e
write_warning_summary "$runner_console_log" "$runner_warning_summary"

bench_status=0
if [[ "$RUN_BREWFS_BENCH" == true ]]; then
    set +e
    run_brewfs_bench "$host_artifact_dir"
    bench_status=$?
    set -e
fi

status=0
if [[ "$container_status" -ne 0 ]]; then
    status="$container_status"
fi
if [[ "$bench_status" -ne 0 ]]; then
    status="$bench_status"
fi

ok "perf compose 运行结束 (container=$container_status, bench=$bench_status)"
ok "产物目录: $host_artifact_dir"
exit "$status"
