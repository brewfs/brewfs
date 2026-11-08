#!/bin/bash
set -u
export PATH=/usr/local/bin:$PATH
cd /home/hxy/brewfs-main || exit 1
echo "=== PR 列表（open）==="
gh pr list -R brewfs/brewfs --state open --limit 8 --json number,title,headRefName \
  --jq '.[] | "  #\(.number) [\(.headRefName[0:34])] \(.title[0:44])"' 2>/dev/null
echo
echo "=== 最近合并 ==="
gh pr list -R brewfs/brewfs --state merged --limit 3 --json number,title,mergedAt \
  --jq '.[] | "  #\(.number) \(.mergedAt) \(.title[0:44])"' 2>/dev/null
echo
echo "=== xattr 文档是否在 main ==="
git fetch origin main -q 2>/dev/null
git cat-file -e origin/main:doc/testing/xattr-backend-support.md 2>/dev/null && echo "  ✅ 已在 main" || echo "  ❌ 不在 main"
