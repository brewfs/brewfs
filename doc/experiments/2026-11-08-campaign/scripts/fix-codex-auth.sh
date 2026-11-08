#!/bin/bash
# 把 brewfs 的 codex 认证（~/.codex）搬到 build 机，然后重启 g424
set -u
K=/home/hxy/.ssh/id_ed25519
SSHO="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $K"

echo "=== 1. brewfs 上的 codex 认证文件 ==="
ls -la /home/hxy/.codex 2>/dev/null | head -6 | sed 's/^/  /'
[ -f /home/hxy/.codex/auth.json ] && echo "  auth.json 存在（$(stat -c%s /home/hxy/.codex/auth.json) B）" || echo "  ❌ 无 auth.json"

echo
echo "=== 2. 同步到 build 机 ==="
$SSHO hxy@172.24.86.194 'mkdir -p /home/hxy/.codex' 2>/dev/null
rsync -a -e "$SSHO" /home/hxy/.codex/ hxy@172.24.86.194:/home/hxy/.codex/ 2>&1 | tail -2 | sed 's/^/  /'
$SSHO hxy@172.24.86.194 'ls -la /home/hxy/.codex | head -5' 2>/dev/null | sed 's/^/  /'

echo
echo "=== 3. 重启 g424 ==="
$SSHO hxy@172.24.86.194 'bash -lc "
  tmux kill-server 2>/dev/null
  /home/hxy/launch-agent-visible.sh g424 /home/hxy/task-424.md /home/hxy/logs/g424.log
"' 2>/dev/null | sed 's/^/  /'

sleep 45
echo
echo "=== 4. 双采样 + pane（去 ANSI）==="
$SSHO hxy@172.24.86.194 'bash -lc "
  tmux ls 2>/dev/null
  stat -c \"  采样1: %s B\" /home/hxy/logs/g424.log 2>/dev/null
  sleep 10
  stat -c \"  采样2: %s B\" /home/hxy/logs/g424.log 2>/dev/null
  echo ---
  tmux capture-pane -p -t g424 2>/dev/null | grep -vE \"^\s*$\" | tail -10 | cut -c1-120
"' 2>/dev/null | sed 's/^/  /'
