#!/bin/bash
set -u
export PATH=/usr/local/bin:$PATH
cd /home/hxy/brewfs-main || exit 1
echo "=== #433 基本信息 ==="
gh pr view 433 -R brewfs/brewfs --json number,state,mergedAt,title,mergeCommit \
  --jq '"  #\(.number) \(.state) mergedAt=\(.mergedAt) sha=\(.mergeCommit.oid[0:8]) \(.title[0:50])"' 2>/dev/null
echo
echo "=== #433 改了哪些文件 ==="
gh pr diff 433 -R brewfs/brewfs --name-only 2>/dev/null | head -12
echo
echo "=== main 上是否已有文档 ==="
git fetch origin main -q 2>/dev/null
git cat-file -e origin/main:doc/testing/xattr-backend-support.md 2>/dev/null && echo "  ✅ 在 main" || echo "  ❌ 不在 main"
echo "  文件大小: $(git cat-file -s origin/main:doc/testing/xattr-backend-support.md 2>/dev/null || echo n/a) B"
