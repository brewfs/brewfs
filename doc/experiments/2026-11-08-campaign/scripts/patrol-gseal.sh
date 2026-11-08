#!/bin/bash
set -u
echo "=== gseal 日志增长 ==="
A=$(stat -c%s /home/hxy/gseal.log 2>/dev/null || echo 0); sleep 10; B=$(stat -c%s /home/hxy/gseal.log 2>/dev/null || echo 0)
echo "  $A -> $B B"
echo "=== 代码改动 ==="
cd /home/hxy/brewfs-main 2>/dev/null && git status --short | head -8 | sed 's/^/  /'
echo "=== 日志尾 ==="
tail -4 /home/hxy/gseal.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
echo "=== 编译进程 ==="
pgrep -af 'cargo build|rustc' 2>/dev/null | head -2 | cut -c1-100 | sed 's/^/  /' || echo "  无"
echo "=== 磁盘/内存 ==="
df -h /home/hxy | tail -1 | sed 's/^/  /'; free -g | head -2 | tail -1 | sed 's/^/  /'
