#!/bin/bash
set -u
echo "=== 编译进程 ==="
ps -eo pid,etime,pcpu,rss,comm,args --sort=-pcpu 2>/dev/null | grep -E 'cargo|rustc' | grep -v grep | head -3 | cut -c1-120 | sed 's/^/  /'
echo "=== 内存/磁盘 ==="
free -g | head -2 | sed 's/^/  /'
df -h /home/hxy | tail -1 | sed 's/^/  /'
echo "=== 构建日志尾 ==="
tail -2 /home/hxy/build-seal.log 2>/dev/null | cut -c1-100 | sed 's/^/  /'
echo "=== 是否已出 BUILD_RC ==="
grep -c BUILD_RC /home/hxy/build-seal.log 2>/dev/null || echo 0
