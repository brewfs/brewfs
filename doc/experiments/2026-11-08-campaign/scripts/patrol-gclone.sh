#!/bin/bash
# 巡检 brewfs 上的 gclone（native clone 修复）
set -u
echo "=== tmux（brewfs）==="
tmux ls 2>/dev/null | sed 's/^/  /'
echo
A=$(stat -c%s /home/hxy/gclone.log 2>/dev/null || echo 0)
sleep 12
B=$(stat -c%s /home/hxy/gclone.log 2>/dev/null || echo 0)
echo "  gclone 日志: $A -> $B B"
echo
echo "=== 最近动作 ==="
tail -6 /home/hxy/gclone.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
echo
echo "=== 代码改动 ==="
cd /home/hxy/brewfs-main 2>/dev/null && { git status --short | head -8 | sed 's/^/  /'; git branch --show-current | sed 's/^/  branch: /'; git log --oneline -2 | sed 's/^/  /'; }
echo
echo "=== 结果文件 ==="
ls -l /home/hxy/CLONE-RESULT.md 2>/dev/null | sed 's/^/  /' || echo "  尚未生成"
