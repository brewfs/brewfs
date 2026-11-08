#!/bin/bash
set -u
K=/home/hxy/.ssh/id_ed25519
SSHO="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $K"
echo "=== $(date '+%H:%M:%S') gxattr 编译/测试细节 ==="
$SSHO hxy@172.24.86.194 'bash -lc "
  echo \"--- cargo/rustc/ld/cc1 ---\"
  ps -eo pid,etime,pcpu,rss,args --sort=-rss 2>/dev/null | grep -E \"cargo|rustc|cc1| ld |collect2\" | grep -v grep | head -5 | cut -c1-140
  echo \"--- target 大小 ---\"
  du -sm /home/hxy/xattr-target 2>/dev/null
  echo \"--- 是否有测试二进制在跑 ---\"
  pgrep -af \"deps/brewfs-\" | grep -v grep | head -3 | cut -c1-120
  echo \"--- 最近改动的产物 ---\"
  find /home/hxy/xattr-target/debug/deps -maxdepth 1 -type f -newermt \"-10 minutes\" 2>/dev/null | head -5
  echo \"--- 磁盘 ---\"; df -h / | tail -1
"' 2>/dev/null | sed 's/^/  /'
