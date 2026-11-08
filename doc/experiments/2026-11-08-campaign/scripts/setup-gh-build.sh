#!/bin/bash
# 在 build 机上准备 gh 认证（复用 brewfs 的 token），以便 agent 能开 PR
set -u
export PATH=/usr/local/bin:$PATH
echo "=== 1. 取 brewfs 的 gh token（只显示前 8 位）==="
TOKEN=$(gh auth token 2>/dev/null || true)
if [ -z "${TOKEN:-}" ]; then echo "  ❌ brewfs 上没有 gh token"; exit 1; fi
echo "  token: ${TOKEN:0:8}...（长度 ${#TOKEN}）"
echo
echo "=== 2. 传到 build 机并登录 ==="
ssh -o StrictHostKeyChecking=no -i /home/hxy/.ssh/id_ed25519 hxy@172.24.86.194 "bash -lc '
  command -v gh >/dev/null 2>&1 || { sudo -n apt-get install -y -qq gh >/dev/null 2>&1; }
  gh --version | head -1
'" 2>&1 | tail -3 | sed 's/^/  /'

printf '%s' "$TOKEN" | ssh -o StrictHostKeyChecking=no -i /home/hxy/.ssh/id_ed25519 hxy@172.24.86.194 \
  "bash -lc 'gh auth login --with-token 2>&1 | tail -2; gh auth status 2>&1 | head -4'" 2>&1 | tail -6 | sed 's/^/  /'

echo
echo "=== 3. 验证 build 机能读仓库 ==="
ssh -o StrictHostKeyChecking=no -i /home/hxy/.ssh/id_ed25519 hxy@172.24.86.194 \
  "bash -lc 'cd /home/hxy/brewfs-main && gh repo view brewfs/brewfs --json nameWithOwner --jq .nameWithOwner'" 2>&1 | tail -2 | sed 's/^/  /'
