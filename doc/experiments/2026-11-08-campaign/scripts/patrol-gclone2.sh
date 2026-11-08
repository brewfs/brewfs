#!/bin/bash
# 更细的巡检：编译状态、代码规模、测试进度
set -u
echo "=== tmux ==="
tmux ls 2>/dev/null | sed 's/^/  /'
echo
A=$(stat -c%s /home/hxy/gclone.log 2>/dev/null || echo 0); sleep 12; B=$(stat -c%s /home/hxy/gclone.log 2>/dev/null || echo 0)
echo "  gclone 日志: $A -> $B B"
echo
echo "=== cargo 进程 ==="
pgrep -af 'cargo|rustc' 2>/dev/null | head -4 | cut -c1-100 | sed 's/^/  /' || echo "  无编译进程"
echo
echo "=== 新增/改动规模 ==="
cd /home/hxy/brewfs-main || exit 1
git status --short | sed 's/^/  /'
echo "  clone.rs 行数: $(wc -l < src/meta/clone.rs 2>/dev/null || echo 0)"
echo
echo "=== 最近日志（找 build/test 关键字）==="
grep -aE 'cargo (build|test|clippy)|error\[E|Compiling brewfs|test result' /home/hxy/gclone.log | tail -6 | cut -c1-120 | sed 's/^/  /'
echo
echo "=== CLONE-RESULT ==="
ls -l /home/hxy/CLONE-RESULT.md 2>/dev/null | sed 's/^/  /' || echo "  尚未生成"
