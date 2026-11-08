#!/bin/bash
set -u
echo "=== $(date '+%H:%M:%S') 磁盘与 target 占用 ==="
df -h /mnt/brewfs-data | tail -1 | sed 's/^/  /'
T=/home/hxy/brewfs-pkv-target
echo "  target 总计: $(du -sm "$T" 2>/dev/null | awk '{print $1}') MB"
for p in debug release; do
  S=$(du -sm "$T/$p" 2>/dev/null | awk '{print $1}')
  echo "    $p: ${S:-0} MB"
done
echo
echo "=== 其他大目录 ==="
du -sm /mnt/brewfs-data/home/hxy/* 2>/dev/null | sort -rn | head -6 | awk '{printf "  %6s MB  %s\n",$1,$2}'
echo
echo "=== agent 状态 ==="
echo "  codex=$(pgrep -fc 'codex exec' || echo 0) rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)"
ps -C rustc -o etime,pcpu,rss --no-headers 2>/dev/null | head -2 | sed 's/^/  /'
echo
echo "=== 内存 ==="
free -g | sed -n 2,3p | sed 's/^/  /'
