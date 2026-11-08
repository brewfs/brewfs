#!/bin/bash
set -u
D=/mnt/brewfs-data/home/hxy/issue-426-20261105
export PATH=/usr/local/bin:$PATH
echo "=== $(date '+%H:%M:%S') #426 验收结果 ==="
echo "--- acceptance 日志尾 8 行 ---"
tail -8 "$D/acceptance.log" 2>/dev/null | cut -c1-125 | sed 's/^/  /'
echo
echo "--- 所有性能结论行 ---"
grep -ahE 'delta .*(regression|CI)' "$D"/*.log 2>/dev/null | tail -10 | cut -c1-160 | sed 's/^/  /'
echo
echo "--- 是否已有 PR ---"
cd /home/hxy/brewfs-main || exit 1
gh pr list -R brewfs/brewfs --state all --limit 8 --json number,title,headRefName --jq '.[] | "  #\(.number) [\(.headRefName[0:32])] \(.title[0:50])"' 2>/dev/null | head -8
echo
echo "--- 分支状态 ---"
git log --oneline -2 | sed 's/^/  /'
echo "  未提交: $(git status --porcelain | wc -l)"
echo
echo "--- agent 是否还在跑 ---"
echo "  codex=$(pgrep -fc 'codex exec' || echo 0)"
tmux capture-pane -p -t view426 2>/dev/null | tail -5 | cut -c1-130 | sed 's/^/  /'
