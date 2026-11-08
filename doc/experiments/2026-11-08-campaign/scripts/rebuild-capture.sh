#!/bin/bash
set -u
cd /home/hxy/brewfs-main || exit 1
export PATH=/home/hxy/shim:/home/hxy/.cargo/bin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export CARGO_TARGET_DIR=/home/hxy/brewfs-pkv-target
export CARGO_BUILD_JOBS=1
export RUSTFLAGS="-C debuginfo=0 -C codegen-units=4"
D=/mnt/brewfs-data/home/hxy/issue424
echo "=== 完整重编，捕获全部输出（jobs=1，避免被 cgroup 杀）==="
cargo test --release --no-default-features \
  --features fuse-tokio-runtime,workspace-overlay,observability \
  --lib --no-run > "$D/rebuild-full.log" 2>&1
RC=$?
echo "  rc=$RC"
echo "--- 错误摘要 ---"
grep -aE '^error(\[|:)|^error\b|cannot find|expected .* found|signal:|SIGKILL|Killed|memory allocation' "$D/rebuild-full.log" 2>/dev/null | head -20 | cut -c1-160 | sed 's/^/  /'
echo "--- 日志尾部 25 行 ---"
tail -25 "$D/rebuild-full.log" 2>/dev/null | cut -c1-140 | sed 's/^/  /'
echo "--- 日志大小 ---"
stat -c '  %s B' "$D/rebuild-full.log" 2>/dev/null
