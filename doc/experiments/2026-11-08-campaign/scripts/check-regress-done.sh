#!/bin/bash
set -u
export PATH=/usr/local/bin:/home/hxy/.cargo/bin:$PATH
cd /home/hxy/brewfs-main || exit 1
echo "=== 1. regress 任务是否完成并提交 ==="
git log --oneline -3 | sed 's/^/  /'
echo "  未提交: $(git status --porcelain | wc -l)"
git status --porcelain | head -5 | sed 's/^/    /'
echo
echo "=== 2. 是否已开 PR ==="
gh pr list -R brewfs/brewfs --state open --json number,isDraft,title --jq '.[] | "  #\(.number) draft=\(.isDraft) \(.title[0:50])"' 2>/dev/null
echo
echo "=== 3. 审计报告是否已生成 ==="
ls -l doc/performance/merged-pr-regression-audit-2026-11-04.md 2>/dev/null | awk '{print "  "$5"B "$9}'
echo
echo "=== 4. 分支是否已推送 ==="
git log --oneline origin/docs/merged-pr-regression-audit-20261104 -1 2>/dev/null | sed 's/^/  origin: /' || echo "  未推送"
