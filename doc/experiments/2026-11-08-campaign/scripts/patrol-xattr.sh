#!/bin/bash
set -u
K=/home/hxy/.ssh/id_ed25519
SSHO="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $K"
echo "=== $(date '+%H:%M:%S') gxattr agent 巡检 ==="
$SSHO hxy@172.24.86.194 'bash -lc "
  echo \"  codex=\$(pgrep -fc codex || echo 0) rustc=\$(pgrep -cx rustc || echo 0) cargo=\$(pgrep -cx cargo || echo 0)\"
  echo \"  内存: \$(free -g | awk \"/Mem:/{print \\\$3\\\"G used / \\\"\\\$7\\\"G avail\\\"}\")\"
  echo \"  --- tmux ---\"
  tmux ls 2>/dev/null
  echo \"  --- 日志双采样 ---\"
  stat -c \"    采样1: %s B\" /home/hxy/logs/gxattr.log 2>/dev/null
  sleep 8
  stat -c \"    采样2: %s B\" /home/hxy/logs/gxattr.log 2>/dev/null
  echo \"  --- pane 尾 12 ---\"
  tmux capture-pane -p -t gxattr 2>/dev/null | grep -vE \"^\s*\$\" | tail -12 | cut -c1-110
  echo \"  --- git（找 xattr worktree）---\"
  for d in /home/hxy/brewfs-main /home/hxy/brewfs-424 /home/hxy/xattr-work; do
    [ -d \"\$d/.git\" -o -f \"\$d/.git\" ] || continue
    echo \"    \$d: \$(cd \$d && git rev-parse --abbrev-ref HEAD) \$(cd \$d && git log --oneline -1 | cut -c1-40)\"
  done
  echo \"  --- 是否已产出文档 ---\"
  ls -l /home/hxy/brewfs-main/doc/testing/xattr-backend-support.md 2>/dev/null || find /home/hxy -maxdepth 5 -name \"xattr-backend-support.md\" 2>/dev/null | head -3
"' 2>/dev/null | sed 's/^/  /'
