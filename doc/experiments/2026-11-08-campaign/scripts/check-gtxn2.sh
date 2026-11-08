#!/bin/bash
set -u
export PATH=/usr/local/bin:$PATH
cd /home/hxy/brewfs-main || exit 1
echo "=== PR 是否存在 ==="
gh pr list -R brewfs/brewfs --head fix/tikv-active-transaction-drop --state all --json number,state,title 2>/dev/null | head -5
echo
echo "=== 本地门禁状态（看 agent 是否跑过）==="
ls -lt /home/hxy/logs/gtxn.log 2>/dev/null | awk '{print "  gtxn.log: "$5" B "$6" "$7" "$8}'
tail -30 /home/hxy/logs/gtxn.log 2>/dev/null | grep -aE 'test result|clippy|fmt|passed|FAILED|error' | tail -8 | cut -c1-150
echo
echo "=== 分支与 main 的差异文件 ==="
git fetch origin main -q 2>/dev/null
git diff --stat origin/main...HEAD 2>/dev/null | tail -10
