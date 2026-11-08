#!/bin/bash
set -u
echo "=== 1. brewfs → 新 TiKV 机的连通性（同 VPC）==="
curl -s --max-time 6 http://172.24.86.193:2379/pd/api/v1/stores 2>/dev/null | head -c 300 | sed 's/^/  /'
echo
echo "  PD ping rc=$?"
echo
echo "=== 2. regress 编译是否结束 ==="
echo "  rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)"
ls /mnt/brewfs-data/home/hxy/regress-20261104/build/complete 2>/dev/null && echo "  ✅ 编译完成" || echo "  🔄 仍在编译"
ls /mnt/brewfs-data/home/hxy/regress-20261104/bin/ 2>/dev/null | sed 's/^/    /'
echo
echo "=== 3. 已有的测试二进制（release/deps）==="
ls -lt /home/hxy/brewfs-pkv-target/release/deps/ 2>/dev/null | grep -E '^-rwx' | head -4 | awk '{printf "  %8.0f MB  %s\n",$5/1048576,$9}'
echo
echo "=== 4. TiKV 测试需要的环境变量 ==="
cd /home/hxy/brewfs-main || exit 1
timeout 30 grep -rn --include='*.rs' 'BREWFS_TEST_TIKV_PD_ENDPOINTS' src/ 2>/dev/null | head -3 | cut -c1-120 | sed 's/^/  /'
