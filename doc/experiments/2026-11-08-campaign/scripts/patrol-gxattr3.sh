#!/bin/bash
set -u
echo "=== gxattr2 日志增长 ==="
A=$(stat -c%s /home/hxy/gxattr2.log 2>/dev/null || echo 0); sleep 8; B=$(stat -c%s /home/hxy/gxattr2.log 2>/dev/null || echo 0)
echo "  $A -> $B B"
echo "=== cargo ==="
pgrep -af 'cargo build' | head -2 | cut -c1-110 | sed 's/^/  /' || echo "  无"
echo "=== 二进制 ==="
ls -lh /home/hxy/brewfs-main/target/release/brewfs 2>/dev/null | awk '{print "  "$5, $6, $7, $8}' || echo "  未产出"
echo "=== 结果文件 ==="
ls -l /home/hxy/XATTR-RESULT.md 2>/dev/null | sed 's/^/  /' || echo "  尚未生成"
echo "=== 日志尾 ==="
tail -5 /home/hxy/gxattr2.log 2>/dev/null | cut -c1-130 | sed 's/^/  /'
