#!/bin/bash
# 补装 protoc 等构建依赖，然后重测
set -u
echo "=== 1. 补装依赖 ==="
sudo -n apt-get install -y -qq protobuf-compiler libprotobuf-dev clang lld 2>&1 | tail -2 | sed 's/^/  /'
echo "  protoc: $(protoc --version 2>/dev/null)"

echo
echo "=== 2. 复测 cargo check ==="
export HOME=/home/hxy
export PATH=/home/hxy/shim:/home/hxy/.cargo/bin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export CARGO_TARGET_DIR=/home/hxy/brewfs-pkv-target
cd /home/hxy/brewfs-main || exit 1
S=$(date +%s)
cargo check --no-default-features --features fuse-tokio-runtime,workspace-overlay --lib 2>&1 | tail -3 | sed 's/^/  /'
E=$(date +%s); echo "  check 用时 $((E-S))s"

echo
echo "=== 3. 复测测试二进制构建 ==="
S=$(date +%s)
cargo test --no-default-features --features fuse-tokio-runtime,workspace-overlay --lib --no-run 2>&1 | tail -3 | sed 's/^/  /'
E=$(date +%s); echo "  构建用时 $((E-S))s"
echo "  target: $(du -sm $CARGO_TARGET_DIR 2>/dev/null | awk '{print $1}') MB"
