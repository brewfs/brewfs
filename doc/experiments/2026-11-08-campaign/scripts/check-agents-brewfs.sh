#!/bin/bash
set -u
export PATH=/usr/local/bin:$PATH
cd /home/hxy/brewfs-main || exit 1
echo "=== main 最近 4 提交 ==="
git log --oneline -4 | sed 's/^/  /'
echo
echo "=== 最近合并的 PR ==="
gh pr list -R brewfs/brewfs --state merged --limit 6 --json number,title,mergedAt \
  --jq '.[] | "  #\(.number) \(.mergedAt) \(.title[0:52])"' 2>/dev/null
echo
echo "=== gtxn 会话（事务修复 agent）==="
tmux capture-pane -p -t gtxn 2>/dev/null | grep -vE '^\s*$' | tail -3 | cut -c1-110 | sed 's/^/  /'
echo
echo "=== 分支状态 ==="
echo "  fix/tikv-active-transaction-drop: $(git rev-parse --verify fix/tikv-active-transaction-drop 2>/dev/null | cut -c1-8) ($(git log --oneline -1 fix/tikv-active-transaction-drop 2>/dev/null | cut -c1-50))"
