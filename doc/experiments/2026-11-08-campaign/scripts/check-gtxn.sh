#!/bin/bash
set -u
export PATH=/usr/local/bin:$PATH
cd /home/hxy/brewfs-main || exit 1
echo "=== 分支/提交 ==="
git rev-parse --abbrev-ref HEAD
git log --oneline -3
echo
echo "=== 改动统计 ==="
git show --stat --oneline HEAD | head -20
echo
echo "=== 新增文件 src/tikv_transaction.rs 摘要 ==="
head -40 src/tikv_transaction.rs 2>/dev/null
echo
echo "=== 是否已推送/有 PR ==="
git ls-remote --heads origin 'fix/tikv-active-transaction-drop' 2>/dev/null | sed 's/^/  remote: /'
gh pr list -R brewfs/brewfs --head fix/tikv-active-transaction-drop --state all --json number,state \
  --jq '.[] | "  PR #\(.number) \(.state)"' 2>/dev/null
