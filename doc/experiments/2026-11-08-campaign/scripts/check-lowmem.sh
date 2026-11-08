#!/bin/bash
set -u
D=/mnt/brewfs-data/home/hxy/issue424
L=$D/runner-lowmem.log
echo "=== $(date '+%H:%M:%S') 低内存重编进度 ==="
echo "  rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)  mem=$(free -g | awk '/Mem:/{print $7}')G"
echo
echo "--- rustc 明细（关键：RSS 是否已降到 ~10G）---"
ps -C rustc -o etime,pcpu,rss,nlwp --no-headers 2>/dev/null | head -3 | awk '{printf "  etime=%s cpu=%s%% rss=%.1fGB threads=%s\n",$1,$2,$3/1048576,$4}'
echo
echo "--- runner-lowmem.log 尾 12 行 ---"
tail -12 "$L" 2>/dev/null | cut -c1-130 | sed 's/^/  /'
echo
echo "--- 日志大小/时间 ---"
stat -c '  %s B  %y' "$L" 2>/dev/null
