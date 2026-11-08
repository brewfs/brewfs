#!/bin/bash
# 在 jump 上安装 codex（有公网出口），并启动 agent 通过 ssh 管 a-client
set -u
echo "=== 1. 安装 codex 到 jump ==="
cd /root
if [ -f /root/codex-min.tgz ]; then
  rm -rf /root/codex-min && mkdir -p /root/codex-min
  tar -xzf /root/codex-min.tgz -C /root/codex-min
  install -m 755 /root/codex-min/bin/codex /usr/local/bin/codex
  mkdir -p /root/.codex
  cp /root/codex-min/codex-home/config.toml /root/.codex/config.toml
  echo "  codex: $(codex --version 2>&1 | head -1)"
else
  echo "  ❌ 缺 /root/codex-min.tgz"
  exit 1
fi

echo
echo "=== 2. 准备可信工作目录 ==="
mkdir -p /root/ax-work
if [ ! -d /root/ax-work/.git ]; then
  git init -q /root/ax-work
  git -C /root/ax-work config user.email agent@local
  git -C /root/ax-work config user.name agent
fi
echo "  git: $(git -C /root/ax-work rev-parse --is-inside-work-tree 2>&1)"

echo
echo "=== 3. 停掉 a-client 上无出口的 codex ==="
ssh -i /root/.ssh/ax-c1-20261106 -o StrictHostKeyChecking=no root@10.20.0.11 'tmux kill-session -t gbrew 2>/dev/null; pkill -f "codex exec" 2>/dev/null; echo "  已停"' 2>&1 | tail -1
