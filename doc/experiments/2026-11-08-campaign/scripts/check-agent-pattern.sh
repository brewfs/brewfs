#!/bin/bash
set -u
echo "=== 1. 现有 agent 启动脚本（找可复用的模式）==="
ls -lt /home/hxy/*.sh 2>/dev/null | head -12 | awk '{print "  "$6" "$7" "$8" "$9}'
echo
echo "--- 含 codex exec 的脚本 ---"
grep -l 'codex exec' /home/hxy/*.sh 2>/dev/null | head -5 | sed 's/^/  /'
echo
echo "=== 2. 随便看一个的启动行 ==="
for f in $(grep -l 'codex exec' /home/hxy/*.sh 2>/dev/null | head -2); do
  echo "  --- $f ---"
  grep -n 'codex exec' "$f" | head -3 | cut -c1-200 | sed 's/^/    /'
done
echo
echo "=== 3. codex 版本与默认模型配置 ==="
export PATH=/usr/local/bin:$PATH
which codex 2>/dev/null | sed 's/^/  /'
codex --version 2>/dev/null | head -1 | sed 's/^/  /'
echo
echo "=== 4. 当前 tmux 会话 ==="
tmux ls 2>/dev/null | sed 's/^/  /'
echo
echo "=== 5. 内存/磁盘 ==="
free -g | sed -n 2p | sed 's/^/  /'
df -h /mnt/brewfs-data | tail -1 | sed 's/^/  /'
