#!/bin/bash
set -u
export PATH=/usr/local/bin:$PATH
cd /home/hxy/brewfs-main || exit 1
echo "=== 1. PR #425 状态 ==="
gh pr view 425 -R brewfs/brewfs --json number,title,isDraft,mergeable,mergeStateStatus,statusCheckRollup --jq '"  #\(.number) draft=\(.isDraft) mergeable=\(.mergeable) state=\(.mergeStateStatus) checks=\((.statusCheckRollup//[])|length)"' 2>/dev/null
echo
echo "=== 2. 报告里的 readdir 段落（看它怎么写的）==="
grep -n -A5 -B2 'readdir' doc/performance/merged-pr-regression-audit-2026-11-04.md 2>/dev/null | head -40 | cut -c1-150 | sed 's/^/  /'
echo
echo "=== 3. 报告结尾的结论/建议 ==="
tail -25 doc/performance/merged-pr-regression-audit-2026-11-04.md 2>/dev/null | cut -c1-150 | sed 's/^/  /'
