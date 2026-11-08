#!/bin/bash
# 修 build 机上 codex 路径问题 + 清理 known_hosts 告警 + 重启 g424
set -u
K=/home/hxy/.ssh/id_ed25519
R="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $K"

echo "=== 1. 清理 brewfs 上对该主机的旧 known_hosts 记录 ==="
ssh-keygen -f /home/hxy/.ssh/known_hosts -R 172.24.86.194 >/dev/null 2>&1
$R hxy@172.24.86.194 'echo "  连接正常: $(hostname)"' 2>/dev/null

echo
echo "=== 2. 建目标目录后重新 rsync ==="
$R hxy@172.24.86.194 'mkdir -p /home/hxy/.local/share/myaiim-codex-test' 2>/dev/null
S=$(date +%s)
rsync -a -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $K" \
  /home/hxy/.local/share/myaiim-codex-test/ hxy@172.24.86.194:/home/hxy/.local/share/myaiim-codex-test/ 2>&1 | tail -2 | sed 's/^/  /'
E=$(date +%s); echo "  同步 $((E-S))s"

echo
echo "=== 3. 验证 codex ==="
$R hxy@172.24.86.194 'bash -lc "/home/hxy/.local/share/myaiim-codex-test/cli/node_modules/.bin/codex --version 2>&1 | head -2"' 2>/dev/null | sed 's/^/  /'

echo
echo "=== 4. 停掉刚才失败的 g424，重新启动 ==="
$R hxy@172.24.86.194 'tmux kill-session -t g424 2>/dev/null; rm -f /home/hxy/logs/g424.log' 2>/dev/null
$R hxy@172.24.86.194 'bash -lc "chmod +x /home/hxy/launch-agent-visible.sh; /home/hxy/launch-agent-visible.sh g424 /home/hxy/task-424.md /home/hxy/logs/g424.log"' 2>/dev/null | tail -3 | sed 's/^/  /'

sleep 30
echo
echo "=== 5. 双采样确认 ==="
$R hxy@172.24.86.194 'bash -lc "
  tmux ls 2>/dev/null
  echo ---
  stat -c \"  采样1: %s B %y\" /home/hxy/logs/g424.log 2>/dev/null
  sleep 8
  stat -c \"  采样2: %s B %y\" /home/hxy/logs/g424.log 2>/dev/null
  echo ---
  tmux capture-pane -p -t g424 2>/dev/null | tail -10 | cut -c1-130
"' 2>/dev/null | sed 's/^/  /'
