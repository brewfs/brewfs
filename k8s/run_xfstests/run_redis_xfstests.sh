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
  - 在 Kubernetes 中运行 redis + rustfs + rustfs-init + xfstests
  - 对象存储固定使用 rustfs（BREWFS_DATA_BACKEND=s3）
  - 本地导出的测试产物输出到: $ARTIFACTS_DIR

选项:
  --image <image>            xfstests runner 镜像，必填
  --namespace <ns>           命名空间，默认: brewfs-xfstests
  --cases "<case...>"        只跑指定用例，例如: "generic/001 generic/002"
  --skip-cases <N>           全量模式下跳过默认测试序列中的前 N 个用例
  --check-args "<args...>"   直接透传给 xfstests ./check 的参数
  --fuse-op-log              导出 FUSE 操作日志
  --rust-log <value>         设置 RUST_LOG，默认: brewfs=info
  --keep                     结束后保留 namespace 资源
  -h, --help                 显示帮助
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

require_non_negative_integer() {
    local option="$1"
    local value="${2:-}"
    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
        err "$option 需要提供非负整数，当前值: ${value:-<empty>}"
        exit 1
    fi
}

K8S_NAMESPACE="brewfs-xfstests"
XFSTESTS_IMAGE=""
RUSTFS_IMAGE="rustfs/rustfs:latest"
AWSCLI_IMAGE="amazon/aws-cli:latest"
XFSTESTS_CASES_VALUE=""
XFSTESTS_SKIP_CASES_VALUE="0"
XFSTESTS_CHECK_ARGS_VALUE=""
BREWFS_FUSE_OP_LOG_VALUE="0"
RUST_LOG_VALUE="brewfs=info"
KEEP=false

while [[ $# -gt 0 ]]; do
    case "${1:-}" in
        --image)
            require_value "$1" "${2:-}"
            XFSTESTS_IMAGE="${2:-}"
            shift 2
            ;;
        --namespace)
            require_value "$1" "${2:-}"
            K8S_NAMESPACE="${2:-}"
            shift 2
            ;;
        --cases)
            require_value "$1" "${2:-}"
            XFSTESTS_CASES_VALUE="${2:-}"
            shift 2
            ;;
        --skip-cases)
            require_value "$1" "${2:-}"
            require_non_negative_integer "$1" "${2:-}"
            XFSTESTS_SKIP_CASES_VALUE="${2:-}"
            shift 2
            ;;
        --check-args)
            require_value "$1" "${2:-}"
            XFSTESTS_CHECK_ARGS_VALUE="${2:-}"
            shift 2
            ;;
        --fuse-op-log)
            BREWFS_FUSE_OP_LOG_VALUE="1"
            shift
            ;;
        --rust-log)
            require_value "$1" "${2:-}"
            RUST_LOG_VALUE="${2:-}"
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

if [[ -z "$XFSTESTS_IMAGE" ]]; then
    err "--image 是必填参数"
    exit 1
fi

if [[ "$XFSTESTS_SKIP_CASES_VALUE" != "0" && -n "$XFSTESTS_CASES_VALUE" ]]; then
    err "--skip-cases 不能与 --cases 同时使用"
    exit 1
fi

if [[ "$XFSTESTS_SKIP_CASES_VALUE" != "0" && -n "$XFSTESTS_CHECK_ARGS_VALUE" ]]; then
    err "--skip-cases 不能与 --check-args 同时使用"
    exit 1
fi

for cmd in kubectl python3; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        err "缺少依赖命令: $cmd"
        exit 1
    fi
done

mkdir -p "$ARTIFACTS_DIR"

cleanup() {
    if [[ "$KEEP" == true ]]; then
        info "保留 namespace 资源 (--keep): $K8S_NAMESPACE"
        return 0
    fi
    kubectl delete namespace "$K8S_NAMESPACE" --ignore-not-found=true >/dev/null 2>&1 || true
}

render_manifests() {
    local render_dir="$1"
    cp "$SCRIPT_DIR"/*.yaml "$render_dir"/
    python3 - "$render_dir" <<'PY'
from pathlib import Path
import os
import sys

render_dir = Path(sys.argv[1])
replacements = {
    "__NAMESPACE__": os.environ["K8S_NAMESPACE"],
    "__XFSTESTS_IMAGE__": os.environ["XFSTESTS_IMAGE"],
    "__RUSTFS_IMAGE__": os.environ["RUSTFS_IMAGE"],
    "__AWSCLI_IMAGE__": os.environ["AWSCLI_IMAGE"],
    "__XFSTESTS_CASES__": os.environ["XFSTESTS_CASES_VALUE"],
    "__XFSTESTS_SKIP_CASES__": os.environ["XFSTESTS_SKIP_CASES_VALUE"],
    "__XFSTESTS_CHECK_ARGS__": os.environ["XFSTESTS_CHECK_ARGS_VALUE"],
    "__BREWFS_FUSE_OP_LOG__": os.environ["BREWFS_FUSE_OP_LOG_VALUE"],
    "__RUST_LOG__": os.environ["RUST_LOG_VALUE"],
}

for path in render_dir.glob("*.yaml"):
    content = path.read_text()
    for old, new in replacements.items():
        content = content.replace(old, new)
    path.write_text(content)
PY
}

apply_base_resources() {
    local render_dir="$1"
    kubectl apply -f "$render_dir/namespace.yaml"
    kubectl -n "$K8S_NAMESPACE" apply -f "$render_dir/pvc.yaml"
    kubectl -n "$K8S_NAMESPACE" apply -f "$render_dir/redis.yaml"
    kubectl -n "$K8S_NAMESPACE" apply -f "$render_dir/rustfs.yaml"
}

wait_base_ready() {
    kubectl -n "$K8S_NAMESPACE" rollout status deployment/redis --timeout=180s
    kubectl -n "$K8S_NAMESPACE" rollout status deployment/rustfs --timeout=180s
}

run_job_and_wait() {
    local _render_dir="$1"
    local job_name="$2"
    local yaml_path="$3"
    local timeout_secs="$4"

    kubectl -n "$K8S_NAMESPACE" delete job "$job_name" --ignore-not-found=true >/dev/null 2>&1 || true
    kubectl -n "$K8S_NAMESPACE" apply -f "$yaml_path"
    kubectl -n "$K8S_NAMESPACE" wait --for=condition=Complete "job/$job_name" --timeout="${timeout_secs}s"
}

stream_job_logs() {
    local job_name="$1"
    local pod_name=""
    local i=0
    for ((i = 0; i < 120; i++)); do
        pod_name="$(kubectl -n "$K8S_NAMESPACE" get pods -l "job-name=$job_name" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
        if [[ -n "$pod_name" ]]; then
            break
        fi
        sleep 1
    done
    if [[ -n "$pod_name" ]]; then
        kubectl -n "$K8S_NAMESPACE" logs -f "$pod_name" || true
    fi
}

export_artifacts() {
    local output_dir="$1"
    local helper_name="artifacts-export"

    kubectl -n "$K8S_NAMESPACE" delete pod "$helper_name" --ignore-not-found=true >/dev/null 2>&1 || true
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $helper_name
  namespace: $K8S_NAMESPACE
spec:
  restartPolicy: Never
  containers:
    - name: exporter
      image: busybox:1.36
      command: ["sh", "-ec", "sleep 3600"]
      volumeMounts:
        - name: artifacts
          mountPath: /artifacts
  volumes:
    - name: artifacts
      persistentVolumeClaim:
        claimName: artifacts
EOF

    kubectl -n "$K8S_NAMESPACE" wait --for=condition=Ready pod/"$helper_name" --timeout=120s
    mkdir -p "$output_dir"
    kubectl -n "$K8S_NAMESPACE" cp "$helper_name:/artifacts/." "$output_dir"
    kubectl -n "$K8S_NAMESPACE" delete pod "$helper_name" --ignore-not-found=true >/dev/null 2>&1 || true
}

main() {
    local render_dir
    local ts
    local local_artifact_dir
    local status=0

    ts="$(date +%s)-$RANDOM"
    local_artifact_dir="$ARTIFACTS_DIR/run-${ts}"
    render_dir="$(mktemp -d)"

    export K8S_NAMESPACE XFSTESTS_IMAGE RUSTFS_IMAGE AWSCLI_IMAGE
    export XFSTESTS_CASES_VALUE XFSTESTS_SKIP_CASES_VALUE
    export XFSTESTS_CHECK_ARGS_VALUE BREWFS_FUSE_OP_LOG_VALUE RUST_LOG_VALUE

    trap cleanup EXIT INT TERM

    info "渲染临时 Kubernetes 清单: $render_dir"
    render_manifests "$render_dir"

    info "应用基础资源: namespace + pvc + redis + rustfs"
    apply_base_resources "$render_dir"

    info "等待 redis 与 rustfs 就绪"
    wait_base_ready

    info "初始化 rustfs bucket"
    run_job_and_wait "$render_dir" "rustfs-init" "$render_dir/rustfs-init-job.yaml" 240

    info "运行 xfstests Job"
    kubectl -n "$K8S_NAMESPACE" delete job xfstests --ignore-not-found=true >/dev/null 2>&1 || true
    kubectl -n "$K8S_NAMESPACE" apply -f "$render_dir/xfstests-job.yaml"
    stream_job_logs "xfstests"

    set +e
    kubectl -n "$K8S_NAMESPACE" wait --for=condition=Complete job/xfstests --timeout=28800s
    status=$?
    set -e

    info "导出 /artifacts 到本地目录: $local_artifact_dir"
    export_artifacts "$local_artifact_dir"

    if [[ "$status" -eq 0 ]]; then
        ok "xfstests PASS"
    else
        err "xfstests FAIL 或超时"
        kubectl -n "$K8S_NAMESPACE" get job xfstests -o wide || true
        kubectl -n "$K8S_NAMESPACE" get pods -l job-name=xfstests -o wide || true
    fi

    ok "本地产物目录: $local_artifact_dir"
    exit "$status"
}

main "$@"
