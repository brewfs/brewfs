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
  - 维护 Redis metadata + S3 object store 场景下的观测性 smoke 测试。
  - 默认依次运行一个轻量 xfstests 用例和一个轻量 perf 组合，并校验 artifact 中的 .stats、Redis diagnostics、perf report。
  - 底层复用 run_redis_xfstests.sh 和 run_redis_perf.sh，不复制 compose 逻辑。

选项:
  --xfstests-only             只运行 xfstests smoke
  --perf-only                 只运行 perf smoke
  --xfstests-cases "<case>"   xfstests 用例，默认: "generic/001"
  --xfstests-check-args "<>"  直接透传给 xfstests ./check；设置后不使用 --xfstests-cases
  --full-xfstests             不限制用例，交给 run_redis_xfstests.sh 跑默认集合
  --perf-tools "<tool...>"    perf 工具列表，默认: "fio-bigwrite fio-bigread metaperf"
  --s3                        perf 使用 rustfs S3（默认）
  --minio                     perf 使用 MinIO
  --local-fs                  perf 使用本地对象目录
  --validate <artifact-dir>   不运行测试，只校验已有 artifact
  --keep                      透传给底层脚本，保留 compose 资源方便调试
  -h, --help                  显示帮助
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
    local pattern="$1"
    find "$ARTIFACTS_DIR" -maxdepth 1 -type d -name "$pattern" -printf '%T@ %p\n' 2>/dev/null \
        | sort -nr \
        | awk 'NR == 1 {print $2}'
}

require_file() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        err "缺少文件: $path"
        return 1
    fi
}

require_glob() {
    local pattern="$1"
    compgen -G "$pattern" >/dev/null || {
        err "缺少匹配文件: $pattern"
        return 1
    }
}

require_contains() {
    local path="$1"
    local needle="$2"
    if ! grep -q "$needle" "$path"; then
        err "文件缺少指标/内容: $path :: $needle"
        return 1
    fi
}

validate_stats_file() {
    local stats_file="$1"
    require_file "$stats_file"
    require_contains "$stats_file" "brewfs_cache_requests_total"
    require_contains "$stats_file" "brewfs_cache_hit_ratio"
    require_contains "$stats_file" "brewfs_writeback_dirty_bytes"
    require_contains "$stats_file" "brewfs_reader_buffer_bytes"
    require_contains "$stats_file" "brewfs_s3_get_ops_total"
    require_contains "$stats_file" "brewfs_s3_put_ops_total"
}

validate_xfstests_artifact() {
    local dir="$1"
    info "校验 xfstests artifact: $dir"
    require_file "$dir/brewfs.log"
    require_file "$dir/backend.yml"
    require_file "$dir/results/check.log"
    validate_stats_file "$dir/diagnostics/stats-xfstests-after.txt"
    require_file "$dir/diagnostics/redis-xfstests-after.txt"
    require_contains "$dir/diagnostics/redis-xfstests-after.txt" "INFO commandstats"
    ok "xfstests artifact 观测性校验通过"
}

validate_perf_artifact() {
    local dir="$1"
    local first_stats
    info "校验 perf artifact: $dir"
    require_file "$dir/perf-summary.tsv"
    require_file "$dir/report.md"
    require_glob "$dir/diagnostics/stats-*-after.txt"
    require_glob "$dir/diagnostics/redis-*-after.txt"
    require_contains "$dir/report.md" "## BrewFS Stats"
    first_stats="$(find "$dir/diagnostics" -maxdepth 1 -type f -name 'stats-*-after.txt' | sort | head -1)"
    validate_stats_file "$first_stats"
    ok "perf artifact 观测性校验通过"
}

validate_artifact() {
    local dir="$1"
    if [[ ! -d "$dir" ]]; then
        err "artifact 目录不存在: $dir"
        return 1
    fi
    case "$(basename "$dir")" in
        perf-run-*|juicefs-perf-run-*) validate_perf_artifact "$dir" ;;
        run-*) validate_xfstests_artifact "$dir" ;;
        *)
            if [[ -f "$dir/perf-summary.tsv" ]]; then
                validate_perf_artifact "$dir"
            else
                validate_xfstests_artifact "$dir"
            fi
            ;;
    esac
}

run_xfstests_smoke() {
    local -a args=()
    if [[ -n "$XFSTESTS_CHECK_ARGS_VALUE" ]]; then
        args+=(--check-args "$XFSTESTS_CHECK_ARGS_VALUE")
    elif [[ "$FULL_XFSTESTS" != true ]]; then
        args+=(--cases "$XFSTESTS_CASES_VALUE")
    fi
    if [[ "$KEEP" == true ]]; then
        args+=(--keep)
    fi

    info "运行 Redis xfstests smoke: ${args[*]:-<default>}"
    bash "$SCRIPT_DIR/run_redis_xfstests.sh" "${args[@]}"
    XFSTESTS_ARTIFACT="$(latest_artifact 'run-*')"
    validate_xfstests_artifact "$XFSTESTS_ARTIFACT"
}

run_perf_smoke() {
    local -a args=("$STORAGE_ARG" --tools "$PERF_TOOLS_VALUE")
    if [[ "$KEEP" == true ]]; then
        args+=(--keep)
    fi

    info "运行 Redis perf smoke: tools=$PERF_TOOLS_VALUE storage=$STORAGE_ARG"
    bash "$SCRIPT_DIR/run_redis_perf.sh" "${args[@]}"
    PERF_ARTIFACT="$(latest_artifact 'perf-run-*')"
    validate_perf_artifact "$PERF_ARTIFACT"
}

RUN_XFSTESTS=true
RUN_PERF=true
FULL_XFSTESTS=false
KEEP=false
XFSTESTS_CASES_VALUE="${OBS_XFSTESTS_CASES:-generic/001}"
XFSTESTS_CHECK_ARGS_VALUE=""
PERF_TOOLS_VALUE="${OBS_PERF_TOOLS:-fio-bigwrite fio-bigread metaperf}"
STORAGE_ARG="--s3"
VALIDATE_ONLY=""
XFSTESTS_ARTIFACT=""
PERF_ARTIFACT=""

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --xfstests-only)
            RUN_XFSTESTS=true
            RUN_PERF=false
            shift
            ;;
        --perf-only)
            RUN_XFSTESTS=false
            RUN_PERF=true
            shift
            ;;
        --xfstests-cases)
            require_value "$1" "${2:-}"
            XFSTESTS_CASES_VALUE="${2:-}"
            FULL_XFSTESTS=false
            shift 2
            ;;
        --xfstests-check-args)
            require_value "$1" "${2:-}"
            XFSTESTS_CHECK_ARGS_VALUE="${2:-}"
            FULL_XFSTESTS=false
            shift 2
            ;;
        --full-xfstests)
            FULL_XFSTESTS=true
            shift
            ;;
        --perf-tools)
            require_value "$1" "${2:-}"
            PERF_TOOLS_VALUE="${2:-}"
            shift 2
            ;;
        --s3)
            STORAGE_ARG="--s3"
            shift
            ;;
        --minio)
            STORAGE_ARG="--minio"
            shift
            ;;
        --local-fs)
            STORAGE_ARG="--local-fs"
            shift
            ;;
        --validate)
            require_value "$1" "${2:-}"
            VALIDATE_ONLY="${2:-}"
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

if [[ -n "$VALIDATE_ONLY" ]]; then
    validate_artifact "$VALIDATE_ONLY"
    exit 0
fi

if [[ "$RUN_XFSTESTS" == true ]]; then
    run_xfstests_smoke
fi
if [[ "$RUN_PERF" == true ]]; then
    run_perf_smoke
fi

ok "Redis observability suite PASS"
if [[ -n "$XFSTESTS_ARTIFACT" ]]; then
    ok "xfstests artifact: $XFSTESTS_ARTIFACT"
fi
if [[ -n "$PERF_ARTIFACT" ]]; then
    ok "perf artifact: $PERF_ARTIFACT"
fi
