#!/bin/bash
set -u
echo "=== brewfs 上的 release 二进制 ==="
ls -lh /home/hxy/brewfs-main/target/release/brewfs 2>/dev/null | awk '{print "  "$5, $6, $7, $8}' || echo "  不存在"
if [ -f /home/hxy/brewfs-main/target/release/brewfs ]; then
  echo "  含新 xattr 代码: $(strings -a /home/hxy/brewfs-main/target/release/brewfs 2>/dev/null | grep -c 'invalid setxattr flags')"
fi
echo "=== 源码是否仍带 patch ==="
cd /home/hxy/brewfs-main 2>/dev/null && git diff --stat | tail -3 | sed 's/^/  /'
echo "=== 磁盘 ==="
df -h /home/hxy | tail -1 | sed 's/^/  /'
