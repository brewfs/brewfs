#!/bin/bash
set -u
L=/mnt/brewfs-data/home/hxy/issue424/runner.log
echo "=== $(date '+%H:%M:%S') xattr runner 状态 ==="
echo "  rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0) fio=$(pgrep -cx fio || echo 0)"
echo "  tmux: $(tmux ls 2>/dev/null | tr '\n' ' ')"
echo
echo "--- runner.log 尾 20 行 ---"
tail -20 "$L" 2>/dev/null | cut -c1-140 | sed 's/^/  /'
echo
echo "--- 日志大小/时间 ---"
stat -c '  %s B  %y' "$L" 2>/dev/null
