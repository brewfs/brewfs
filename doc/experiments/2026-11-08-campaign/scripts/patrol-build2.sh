#!/bin/bash
set -u
echo "=== 二进制 ==="
ls -lh /home/hxy/brewfs-main/target/release/brewfs 2>/dev/null | awk '{print "  "$5, $6, $7, $8}' || echo "  尚未产出"
echo
echo "=== rustc 当前在编什么 ==="
pgrep -af rustc | head -2 | cut -c1-110 | sed 's/^/  /' || echo "  无 rustc"
echo
echo "=== cargo 进程数 ==="
pgrep -fc 'cargo build' || echo 0
echo
echo "=== release 目录更新时间 ==="
ls -lt /home/hxy/brewfs-main/target/release/ 2>/dev/null | head -4 | awk '{print "  "$6" "$7" "$8"  "$9}'
echo
echo "=== gbench 日志增长 ==="
A=$(stat -c%s /home/hxy/gbench.log); sleep 20; B=$(stat -c%s /home/hxy/gbench.log)
echo "  $A -> $B B"
tail -6 /home/hxy/gbench.log | cut -c1-110 | sed 's/^/  /'
