#!/bin/bash
set -u
export PATH=/usr/local/bin:$PATH
K=/home/hxy/.ssh/id_ed25519
SSHO="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $K"
echo "=== $(date '+%H:%M:%S') gxattr 收尾状态 ==="
$SSHO hxy@172.24.86.194 'bash -lc "
  echo \"  codex=\$(pgrep -fc codex || echo 0) rustc=\$(pgrep -cx rustc || echo 0)\"
  echo \"  --- 分支/改动 ---\"
  cd /home/hxy/brewfs-main 2>/dev/null && { git rev-parse --abbrev-ref HEAD; git log --oneline -2; echo \"  dirty=\$(git status --porcelain | wc -l)\"; git status --porcelain | head -6; }
  echo \"  --- xattr 文档 ---\"
  ls -l doc/testing/xattr-backend-support.md 2>/dev/null
  echo \"  --- 是否含 JuiceFS 段 ---\"
  grep -c -i juicefs doc/testing/xattr-backend-support.md 2>/dev/null
  echo \"  --- PR ---\"
  gh pr list -R brewfs/brewfs --head docs/xattr-backend-support --state all --json number,state --jq \".[] | \\\"#\\(.number) \\(.state)\\\"\" 2>/dev/null
"' 2>/dev/null | sed 's/^/  /'
