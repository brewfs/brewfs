#!/bin/bash
# 重启 xattr 构建：走 shim（cgroup 保护）+ codegen-units=4
set -u
D=/mnt/brewfs-data/home/hxy/issue424
L=$D/rebuild-cgroup.log
exec >> "$L" 2>&1
echo "=== $(date '+%F %T') 重启构建（shim + codegen-units=4，cgroup 保护）==="

# 清场
for p in $(pgrep -x cargo) $(pgrep -x rustc); do kill -9 "$p" 2>/dev/null || true; done
sleep 2

cd /home/hxy/brewfs-main || exit 1
export PATH=/home/hxy/shim:/home/hxy/.cargo/bin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export CARGO_TARGET_DIR=/home/hxy/brewfs-pkv-target
export CARGO_BUILD_JOBS=2
export CARGO_PROFILE_RELEASE_DEBUG=0
export CARGO_INCREMENTAL=0
export RUSTFLAGS="-C debuginfo=0 -C codegen-units=4"
export BREWFS_TEST_TIKV_PD_ENDPOINTS=172.24.86.193:2379

echo "--- 环境 ---"
echo "  mem=$(free -g | awk '/Mem:/{print $7}')G  target=$(du -sm /home/hxy/brewfs-pkv-target 2>/dev/null | awk '{print $1}')MB"
echo "--- PD ---"
curl -s --max-time 5 "http://172.24.86.193:2379/pd/api/v1/stores" | head -c 100; echo

echo "--- 构建（走 shim，MemoryMax=26G）---"
S=$(date +%s)
cargo test --release --no-default-features \
  --features fuse-tokio-runtime,workspace-overlay,observability \
  --lib --no-run > "$D/build-cgroup.log" 2>&1
RC=$?; E=$(date +%s)
echo "  build rc=$RC 用时 $((E-S))s"
if [ "$RC" -ne 0 ]; then
  echo "--- 错误尾 30 行 ---"
  tail -30 "$D/build-cgroup.log" | cut -c1-160
  exit 1
fi

echo "--- 跑 xattr ---"
S=$(date +%s)
cargo test --release --no-default-features \
  --features fuse-tokio-runtime,workspace-overlay,observability \
  --lib -- --ignored --nocapture xattr > "$D/xattr-run.log" 2>&1
RC=$?; E=$(date +%s)
echo "  xattr rc=$RC 用时 $((E-S))s"
echo "--- xattr 输出尾 100 行 ---"
tail -100 "$D/xattr-run.log" | cut -c1-160
echo "=== $(date '+%F %T') 结束 ==="
