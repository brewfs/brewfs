#!/bin/bash
set -u
echo "=== 1. 是否已有编译好的测试二进制（可直接跑，不必再编）==="
T=/home/hxy/brewfs-pkv-target
ls -lt "$T"/debug/deps/*tikv* 2>/dev/null | head -5 | awk '{printf "  %s %s\n",$6" "$7" "$8,$9}'
find "$T" -maxdepth 3 -name 'brewfs-*' -type f -newermt '-3 days' 2>/dev/null | head -5 | while read -r p; do
  printf "  %.0fMB %s\n" "$(awk "BEGIN{print $(stat -c%s "$p")/1048576}")" "$p"
done
echo
echo "=== 2. 测试的 #[ignore] / 环境门槛（决定它是否会跳过）==="
cd /home/hxy/brewfs-main || exit 1
sed -n '1700,1730p' src/workspace_overlay/stores/tikv.rs | sed 's/^/  /'
echo
echo "=== 3. 当前是否有 TiKV 在跑/容器 ==="
echo "  进程: $(pgrep -c tikv-server 2>/dev/null || echo 0)"
docker ps --format '  {{.Names}} {{.Status}}' 2>/dev/null | head -6
echo
echo "=== 4. 该测试需要的环境变量 ==="
timeout 30 grep -nE 'env::var|BREWFS_.*TIKV|PD_ENDPOINT|tikv' src/workspace_overlay/stores/tikv.rs 2>/dev/null | grep -E 'env::var|ENDPOINT' | head -8 | cut -c1-130 | sed 's/^/  /'
