#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

usage() {
    cat <<EOF
用法: $(basename "$0") [选项]

说明:
  - 独立运行 Redis metadata 性能测试，底层复用 run_redis_perf.sh
  - 默认只跑元数据相关工具: "dirstress dirperf metaperf looptest"
  - 测试产物输出到: $ARTIFACTS_DIR/perf-run-*

选项:
  --s3                       使用 rustfs 作为对象存储（默认）
  --minio                    使用 MinIO 作为对象存储
  --local-fs                 改为使用本地目录作为对象存储
  --s3-writeback             启用 S3 commit-before-upload 写回语义
  --writeback-throughput-profile
                             启用已验证的 S3 writeback 吞吐 profile
  --tools "<tool...>"        指定元数据工具列表，默认: "dirstress dirperf metaperf looptest"
  --quick                    使用较短元数据参数，适合 smoke/debug
  --brewfs-bench             额外运行宿主机 cargo bench --bench brewfs_bench
  --bench-args "<args...>"   透传给 cargo bench 之后的 Criterion 参数
  --keep                     结束后不执行 compose down（便于调试）
  -h, --help                 显示帮助

支持的 metadata PERF_TOOLS:
  dirstress dirperf metaperf looptest

说明:
  - 如需读写 fio，请继续使用 run_redis_perf.sh。
  - 本脚本的 --tools 仅允许元数据工具，避免 metadata baseline 混入读写负载。
  - --quick 默认覆盖未设置的 PERF_DIRSTRESS_*、PERF_METAPERF_*、PERF_LOOPTEST_*。
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

latest_artifact() {
    find "$ARTIFACTS_DIR" -maxdepth 1 -type d -name 'perf-run-*' -printf '%T@ %p\n' 2>/dev/null \
        | sort -nr \
        | awk 'NR == 1 {print $2}'
}

validate_metadata_tools() {
    local tools="$1"
    local tool
    read -r -a parsed_tools <<<"$tools"
    if [[ "${#parsed_tools[@]}" -eq 0 ]]; then
        err "metadata 工具列表不能为空"
        exit 1
    fi
    for tool in "${parsed_tools[@]}"; do
        case "$tool" in
            dirstress|dirperf|metaperf|looptest) ;;
            *)
                err "不支持的 metadata 工具: $tool"
                err "如需 fio 读写测试，请使用 run_redis_perf.sh 或修改本脚本的允许列表。"
                exit 1
                ;;
        esac
    done
}

apply_quick_defaults() {
    export PERF_DIRSTRESS_PROCS="${PERF_DIRSTRESS_PROCS:-2}"
    export PERF_DIRSTRESS_FILES="${PERF_DIRSTRESS_FILES:-50}"
    export PERF_DIRSTRESS_PROCS_PER_DIR="${PERF_DIRSTRESS_PROCS_PER_DIR:-1}"
    export PERF_METAPERF_SECONDS="${PERF_METAPERF_SECONDS:-10}"
    export PERF_METAPERF_OP_FILES="${PERF_METAPERF_OP_FILES:-80}"
    export PERF_METAPERF_BG_FILES="${PERF_METAPERF_BG_FILES:-400}"
    export PERF_LOOPTEST_ITERS="${PERF_LOOPTEST_ITERS:-50}"
}

summarize_latest_artifact() {
    local artifact
    artifact="$(latest_artifact)"
    if [[ -z "$artifact" ]]; then
        err "未找到 perf-run artifact"
        return 1
    fi
    info "metadata perf artifact: $artifact"
    if [[ -f "$artifact/perf-summary.tsv" ]]; then
        sed -n '1,80p' "$artifact/perf-summary.tsv"
    fi
}

PERF_TOOLS_VALUE="${PERF_META_TOOLS:-dirstress dirperf metaperf looptest}"
QUICK=false
RUNNER_ARGS=()

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --s3|--minio|--local-fs|--s3-writeback|--writeback-throughput-profile|--brewfs-bench|--keep)
            RUNNER_ARGS+=("$1")
            shift
            ;;
        --tools)
            require_value "$1" "${2:-}"
            PERF_TOOLS_VALUE="${2:-}"
            shift 2
            ;;
        --quick)
            QUICK=true
            shift
            ;;
        --bench-args)
            require_value "$1" "${2:-}"
            RUNNER_ARGS+=("$1" "$2")
            shift 2
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

validate_metadata_tools "$PERF_TOOLS_VALUE"

if [[ "$QUICK" == true ]]; then
    apply_quick_defaults
fi

info "运行 Redis metadata perf: tools=$PERF_TOOLS_VALUE quick=$QUICK"
bash "$SCRIPT_DIR/run_redis_perf.sh" "${RUNNER_ARGS[@]}" --tools "$PERF_TOOLS_VALUE"
summarize_latest_artifact
ok "Redis metadata perf 完成"
