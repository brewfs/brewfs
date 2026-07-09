#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(realpath "$SCRIPT_DIR/..")"
PROJECT_DIR="$(realpath "$DOCKER_DIR/..")"

COMPOSE_FILE="$SCRIPT_DIR/docker-compose.tikv-perf.yml"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

usage() {
    cat <<EOF
用法: $(basename "$0") [选项]

说明:
  - 使用 docker compose 在容器内运行 xfstests 压力工具，元数据库为 TiKV
  - 默认使用 rustfs 作为对象存储后端
  - 可选附带运行宿主机上的 brewfs_bench
  - 测试产物输出到: $ARTIFACTS_DIR/perf-run-*
  - 当前 TiKV MetaStore 仍是第一版骨架；该脚本用于持续维护性能入口并暴露真实失败点
  - 本地重复调试可设置 BREWFS_REUSE_HOST_BINARY=1 复用 target/docker/brewfs

选项:
  --s3                       使用 rustfs 作为对象存储（默认）
  --local-fs                 改为使用本地目录作为对象存储
  --tools "<tool...>"        指定压力工具列表，默认: "fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"
  --namespace <NAME>         TiKV metadata key namespace，默认: brewfs
  --brewfs-bench           额外运行一次宿主机 cargo bench --bench brewfs_bench
  --bench-args "<args...>"   透传给 cargo bench 之后的 Criterion 参数
  --keep                     结束后不执行 compose down（便于调试）
  -h, --help                 显示帮助

支持的 PERF_TOOLS:
  fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw fio dirstress dirperf metaperf looptest

可通过环境变量覆盖各工具参数:
  PERF_DIRSTRESS_ARGS PERF_DIRPERF_ARGS PERF_METAPERF_ARGS PERF_LOOPTEST_ARGS
  PERF_FIO_ARGS PERF_FIO_RUNTIME PERF_FIO_SIZE PERF_FIO_BS PERF_FIO_NUMJOBS
  PERF_FIO_SEQREAD_ARGS PERF_FIO_SEQWRITE_ARGS PERF_FIO_RANDREAD_ARGS PERF_FIO_RANDWRITE_ARGS PERF_FIO_RANDRW_ARGS
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
USE_S3=true
RUN_BREWFS_BENCH=false
PERF_TOOLS_VALUE="fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest"
BENCH_ARGS_VALUE=""
TIKV_NAMESPACE_VALUE="${BREWFS_META_TIKV_NAMESPACE:-brewfs}"

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --s3)
            USE_S3=true
            shift
            ;;
        --local-fs)
            USE_S3=false
            shift
            ;;
        --tools)
            require_value "$1" "${2:-}"
            PERF_TOOLS_VALUE="${2:-}"
            shift 2
            ;;
        --namespace)
            require_value "$1" "${2:-}"
            TIKV_NAMESPACE_VALUE="${2:-}"
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

mkdir -p "$ARTIFACTS_DIR"

preclean_ports() {
    local -a ports=("${TIKV_PD_HOST_PORT:-12379}" "${TIKV_STATUS_HOST_PORT:-20180}" "${RUSTFS_S3_HOST_PORT:-19000}" "${RUSTFS_CONSOLE_HOST_PORT:-19001}")
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

run_brewfs_bench() {
    local host_artifact_dir="$1"
    local bench_artifact_dir="$host_artifact_dir/brewfs-bench"
    local benchmark_pd_endpoints="127.0.0.1:${TIKV_PD_HOST_PORT:-12379}"
    local -a bench_args=()

    mkdir -p "$bench_artifact_dir"
    if [[ -n "$BENCH_ARGS_VALUE" ]]; then
        read -r -a bench_args <<<"$BENCH_ARGS_VALUE"
    fi

    info "运行宿主机 brewfs_bench（tikv backend）"
    set +e
    (
        cd "$PROJECT_DIR"
        env \
            RUST_LOG="${RUST_LOG:-warn}" \
            BREWFS_BENCH_META_BACKEND=tikv \
            BREWFS_BENCH_META_TIKV_PD_ENDPOINTS="$benchmark_pd_endpoints" \
            BREWFS_BENCH_META_TIKV_NAMESPACE="$TIKV_NAMESPACE_VALUE" \
            BREWFS_BENCH_BACKEND="$([[ "$USE_S3" == true ]] && echo s3 || echo local)" \
            BREWFS_BENCH_S3_BUCKET="${BREWFS_S3_BUCKET:-brewfs-data}" \
            BREWFS_BENCH_S3_REGION="${BREWFS_S3_REGION:-us-east-1}" \
            BREWFS_BENCH_S3_ENDPOINT="http://127.0.0.1:${RUSTFS_S3_HOST_PORT:-19000}" \
            BREWFS_BENCH_S3_FORCE_PATH_STYLE=true \
            AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-rustfsadmin}" \
            AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-rustfsadmin}" \
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

info "构建宿主机 brewfs release 二进制（供镜像 COPY）"
bash "$DOCKER_DIR/build_brewfs_host_binary.sh"

info "构建 perf runner 镜像"
docker compose -f "$COMPOSE_FILE" build perf

ts="$(date +%s)-$RANDOM"
host_artifact_dir="$ARTIFACTS_DIR/perf-run-${ts}"
mkdir -p "$host_artifact_dir"

export BREWFS_ARTIFACT_DIR="/artifacts/perf-run-${ts}"
export BREWFS_S3_BUCKET="${BREWFS_S3_BUCKET:-brewfs-data}"
export BREWFS_META_TIKV_NAMESPACE="$TIKV_NAMESPACE_VALUE"
if [[ "$USE_S3" == true ]]; then
    export BREWFS_DATA_BACKEND="s3"
else
    export BREWFS_DATA_BACKEND="local-fs"
fi

services=(pd tikv tikv-ready)
if [[ "$USE_S3" == true ]]; then
    services+=(rustfs)
fi
info "启动依赖服务: ${services[*]}"
docker compose -f "$COMPOSE_FILE" up -d "${services[@]}"

if [[ "$USE_S3" == true ]]; then
    info "初始化 rustfs bucket（一次性容器）"
    docker compose -f "$COMPOSE_FILE" run --rm rustfs-init
fi

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
    -e PERF_FIO_ARGS \
    -e PERF_FIO_SEQREAD_ARGS \
    -e PERF_FIO_SEQWRITE_ARGS \
    -e PERF_FIO_RANDREAD_ARGS \
    -e PERF_FIO_RANDWRITE_ARGS \
    -e PERF_FIO_RANDRW_ARGS \
    -e PERF_FIO_NAME \
    -e PERF_FIO_RW \
    -e PERF_FIO_RWMIXREAD \
    -e PERF_FIO_BS \
    -e PERF_FIO_SIZE \
    -e PERF_FIO_NUMJOBS \
    -e PERF_FIO_IOENGINE \
    -e PERF_FIO_IODEPTH \
    -e PERF_FIO_DIRECT \
    -e PERF_FIO_RUNTIME \
    -e PERF_FUSE_OPS_LOG \
    -e BREWFS_FUSE_OP_LOG \
    -e BREWFS_FUSE_WORKERS \
    -e BREWFS_FUSE_MAX_BACKGROUND \
    -e BREWFS_NOFILE_LIMIT \
    -e BREWFS_S3_PART_SIZE \
    -e BREWFS_S3_MAX_CONCURRENCY \
    -e BREWFS_COMPRESSION \
    -e BREWFS_VFS_TIMING \
    -e PERF_LOG_TO_CONSOLE \
    perf
container_status=$?
set -e

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
