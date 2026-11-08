#!/bin/bash
set -u
K=/home/hxy/.ssh/id_ed25519
SSHO="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $K"
echo "=== $(date '+%H:%M:%S') gxattr 是否真在推进 ==="
$SSHO hxy@172.24.86.194 'bash -lc "
  echo \"--- rustc/cargo 明细 ---\"
  ps -eo pid,etime,pcpu,rss,args --sort=-rss 2>/dev/null | grep -E \"rustc|cargo|brewfs-\" | grep -v grep | head -6 | cut -c1-150
  echo \"--- 测试二进制是否在跑 ---\"
  pgrep -af \"brewfs-379fdec|--test-threads\" | head -3 | cut -c1-140
  echo \"--- xattr-target 大小 ---\"
  du -sm /home/hxy/xattr-target 2>/dev/null
  echo \"--- 该 agent 的 cargo 日志文件（找 test 输出）---\"
  ls -lt /home/hxy/logs/*.log 2>/dev/null | head -4
"' 2>/dev/null | sed 's/^/  /'
