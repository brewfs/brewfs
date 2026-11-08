#!/bin/bash
set -u
echo "=== tmux（brewfs）==="
tmux ls 2>/dev/null | sed 's/^/  /'
A=$(stat -c%s /home/hxy/gxattr2.log 2>/dev/null || echo 0); sleep 10; B=$(stat -c%s /home/hxy/gxattr2.log 2>/dev/null || echo 0)
echo "  gxattr2 日志: $A -> $B B"
echo "=== 代码改动 ==="
cd /home/hxy/brewfs-main 2>/dev/null && git status --short | head -8 | sed 's/^/  /'
echo "=== 日志尾 ==="
tail -4 /home/hxy/gxattr2.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
