#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || realpath "$SCRIPT_DIR/..")"
TARGET_DIR="${CARGO_TARGET_DIR:-$PROJECT_DIR/target}"
case "$TARGET_DIR" in
    /*) ;;
    *) TARGET_DIR="$PROJECT_DIR/$TARGET_DIR" ;;
esac
BIN_PATH="$TARGET_DIR/release/brewfs"
DOCKER_BIN_PATH="$PROJECT_DIR/target/docker/brewfs"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { log "INFO  $*"; }
ok()   { log "OK    $*"; }
err()  { log "ERROR $*" >&2; }

is_true() {
    case "${1:-}" in
        1|true|TRUE|yes|YES|on|ON) return 0 ;;
        *) return 1 ;;
    esac
}

pick_strip_tool() {
    if command -v llvm-strip >/dev/null 2>&1; then
        echo llvm-strip
        return 0
    fi
    if command -v strip >/dev/null 2>&1; then
        echo strip
        return 0
    fi
    return 1
}

cd "$PROJECT_DIR"

reuse_binary="${BREWFS_REUSE_HOST_BINARY:-${BREWFS_SKIP_HOST_BUILD:-}}"
if is_true "$reuse_binary"; then
    if [[ -x "$DOCKER_BIN_PATH" ]]; then
        ok "复用 Docker build context 二进制: $DOCKER_BIN_PATH"
        exit 0
    fi
    if [[ ! -x "$BIN_PATH" ]]; then
        err "BREWFS_REUSE_HOST_BINARY=1 但未找到可执行二进制: $DOCKER_BIN_PATH 或 $BIN_PATH"
        exit 1
    fi
    info "复用宿主机 release 二进制: $BIN_PATH"
else
    info "在宿主机构建 brewfs release 二进制"
    build_args=(--release -p brewfs --bin brewfs)
    if [[ -n "${BREWFS_CARGO_BUILD_ARGS:-}" ]]; then
        read -r -a extra_args <<<"$BREWFS_CARGO_BUILD_ARGS"
        build_args+=("${extra_args[@]}")
    fi
    cargo build "${build_args[@]}"
fi

if [[ ! -f "$BIN_PATH" ]]; then
    err "构建完成后未找到二进制: $BIN_PATH"
    exit 1
fi

before_size=$(stat -c%s "$BIN_PATH")

mkdir -p "$(dirname "$DOCKER_BIN_PATH")"
install -m 755 "$BIN_PATH" "$DOCKER_BIN_PATH"

if strip_tool=$(pick_strip_tool); then
    info "去除符号表: $strip_tool"
    if ! "$strip_tool" --strip-debug --strip-unneeded "$DOCKER_BIN_PATH" 2>/dev/null; then
        if ! "$strip_tool" --strip-unneeded "$DOCKER_BIN_PATH" 2>/dev/null; then
            "$strip_tool" "$DOCKER_BIN_PATH"
        fi
    fi
else
    err "未找到 strip 工具，请安装 binutils 或 llvm"
    exit 1
fi

after_size=$(stat -c%s "$DOCKER_BIN_PATH")
chmod 755 "$BIN_PATH"
chmod 755 "$DOCKER_BIN_PATH"

ok "宿主机二进制已就绪: $BIN_PATH"
info "Docker build context 二进制已同步: $DOCKER_BIN_PATH"
info "二进制大小: ${before_size} -> ${after_size} 字节"
