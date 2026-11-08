#!/bin/bash
# 修 build 机上 node 路径（自动探测）+ 重启 g424
set -u
K=/home/hxy/.ssh/id_ed25519
R="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $K"

echo "=== 1. build 机上的 node 目录 ==="
$R hxy@172.24.86.194 'ls -d /home/hxy/.local/share/myaiim-codex-test/node* 2>/dev/null; ls /home/hxy/.local/share/myaiim-codex-test/ | head -8' 2>/dev/null | sed 's/^/  /'

echo
echo "=== 2. 找 node 可执行文件 ==="
$R hxy@172.24.86.194 'find /home/hxy/.local/share/myaiim-codex-test -maxdepth 3 -name node -type f 2>/dev/null | head -3' 2>/dev/null | sed 's/^/  /'

echo
echo "=== 3. 改 launch 脚本：自动探测 node ==="
$R hxy@172.24.86.194 'bash -lc "
  CO=/home/hxy/.local/share/myaiim-codex-test
  NODE_DIR=\$(dirname \$(find \$CO -maxdepth 3 -name node -type f | head -1))
  echo \"  NODE_DIR=\$NODE_DIR\"
  sed -i \"s|export PATH=.*node-v24.22.0-linux-x64/bin:.*|export PATH=\$NODE_DIR:/home/hxy/shim:/home/hxy/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin|\" /home/hxy/launch-agent-visible.sh
  grep -n \"export PATH\" /home/hxy/launch-agent-visible.sh
"' 2>/dev/null | sed 's/^/  /'

echo
echo "=== 4. 重启 g424 ==="
$R hxy@172.24.86.194 'tmux kill-session -t g424 2>/dev/null; rm -f /home/hxy/logs/g424.log; /home/hxy/launch-agent-visible.sh g424 /home/hxy/task-424.md /home/hxy/logs/g424.log' 2>/dev/null | tail -3 | sed 's/^/  /'

sleep 35
echo
echo "=== 5. 双采样 + pane ==="
$R hxy@172.24.86.194 'bash -lc "
  tmux ls 2>/dev/null
  stat -c \"  采样1: %s B\" /home/hxy/logs/g424.log 2>/dev/null
  sleep 8
  stat -c \"  采样2: %s B\" /home/hxy/logs/g424.log 2>/dev/null
  echo ---
  tmux capture-pane -p -t g424 2>/dev/null | tail -12 | cut -c1-130
"' 2>/dev/null | sed 's/^/  /'
