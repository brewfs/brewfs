#!/bin/bash
set -u
echo "=== $(date '+%H:%M:%S') gtxn（TiKV 事务修复）状态 ==="
echo "  codex=$(pgrep -fc 'codex' || echo 0) rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)"
echo "  mem=$(free -g | awk '/Mem:/{print $7}')G"
echo
echo "--- tmux ---"
tmux ls 2>/dev/null | sed 's/^/  /'
echo
echo "--- 日志双采样 ---"
stat -c '  采样1: %s B  %y' /home/hxy/logs/gtxn.log 2>/dev/null
sleep 8
stat -c '  采样2: %s B  %y' /home/hxy/logs/gtxn.log 2>/dev/null
echo
echo "--- pane 尾 14 ---"
tmux capture-pane -p -t gtxn 2>/dev/null | grep -vE '^\s*$' | tail -14 | cut -c1-120
echo
echo "--- 分支与改动 ---"
cd /home/hxy/brewfs-main 2>/dev/null && {
  echo "  分支: $(git rev-parse --abbrev-ref HEAD)"
  echo "  最近提交:"; git log --oneline -2 | sed 's/^/    /'
  echo "  与 txn 相关的改动:"; git status --porcelain | grep -iE 'tikv|txn|transaction' | head -6 | sed 's/^/    /'
}
