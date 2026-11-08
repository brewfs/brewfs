#!/bin/bash
set -u
L=/mnt/brewfs-data/home/hxy/issue424/runner.log
echo "=== $(date '+%H:%M:%S') xattr 测试进度 ==="
echo "  rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)  mem=$(free -g | awk '/Mem:/{print $7}')G"
echo
echo "--- runner.log 尾 12 行 ---"
tail -12 "$L" 2>/dev/null | cut -c1-130 | sed 's/^/  /'
echo
echo "--- 日志大小 ---"
stat -c '  %s B  %y' "$L" 2>/dev/null
echo
echo "--- rustc 明细 ---"
ps -C rustc -o etime,pcpu,rss --no-headers 2>/dev/null | head -3 | sed 's/^/  /'
