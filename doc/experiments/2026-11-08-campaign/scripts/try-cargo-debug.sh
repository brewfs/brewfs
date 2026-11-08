#!/bin/bash
set -u
cd /home/hxy/brewfs-main || { echo "cd failed"; exit 1; }
echo "=== 环境 ==="
echo "  cargo: $(ls -l /home/hxy/.cargo/bin/cargo 2>/dev/null | cut -c1-60)"
echo "  git: $(git rev-parse --short HEAD 2>/dev/null)"
echo "=== 试跑 cargo（前台 60 秒）==="
export HOME=/home/hxy
export PATH=/home/hxy/.cargo/bin:$PATH
export CARGO_PROFILE_DEV_DEBUG=0
timeout 60 cargo build --bin brewfs -j 2 --features fuse-io-uring-runtime,workspace-overlay 2>&1 | tail -12 | cut -c1-140 | sed 's/^/  /'
echo "  rc=$?"
