#!/bin/bash
set -u
D=/mnt/brewfs-data/home/hxy/regress-20261104
echo "=== $(date '+%H:%M:%S') 测量进度 ==="
echo "  codex=$(pgrep -fc 'codex exec') rustc=$(pgrep -cx rustc || echo 0) fio=$(pgrep -cx fio || echo 0)"
echo
echo "--- campaign.log 尾 8 行 ---"
tail -8 "$D/campaign.log" 2>/dev/null | cut -c1-140 | sed 's/^/  /'
echo
echo "--- 结果目录 ---"
ls -lt --time-style=+%H:%M "$D" 2>/dev/null | head -8 | awk '{printf "  %s %10s %s\n",$6,$5,$7}'
echo
echo "--- 已产出的测量目录（若有）---"
find "$D" -maxdepth 2 -type d -newermt '-60 minutes' 2>/dev/null | head -8 | sed 's/^/  /'
echo
echo "--- 内存/磁盘 ---"
echo "  mem=$(free -g | awk '/Mem:/{print $7}')G disk=$(df -h /mnt/brewfs-data | tail -1 | awk '{print $4}')"
