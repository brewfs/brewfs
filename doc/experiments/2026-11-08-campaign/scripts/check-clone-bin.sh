#!/bin/bash
set -u
B=/home/hxy/brewfs-main/target/release/brewfs
echo "=== 二进制 ==="
ls -lh $B | awk '{print "  "$5, $6, $7, $8}'
sha256sum $B | awk '{print "  sha256 "$1}'
echo
echo "=== clone 子命令 ==="
timeout 30 $B clone --help 2>&1 | head -20 | sed 's/^/  /'
echo
echo "=== 顶层 help 里的 clone ==="
timeout 30 $B --help 2>&1 | grep -a -A1 -i clone | head -6 | sed 's/^/  /'
echo
echo "=== 磁盘 ==="
df -h /home/hxy | tail -1 | sed 's/^/  /'
