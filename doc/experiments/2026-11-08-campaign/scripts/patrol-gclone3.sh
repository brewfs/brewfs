#!/bin/bash
set -u
echo "=== gclone 日志尾 ==="
tail -12 /home/hxy/gclone.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
echo
echo "=== 日志两次采样 ==="
A=$(stat -c%s /home/hxy/gclone.log); sleep 25; B=$(stat -c%s /home/hxy/gclone.log)
echo "  $A -> $B B"
echo
echo "=== codex 进程 CPU ==="
ps -o pid,etime,pcpu,stat,rss,args -p 1800333 2>/dev/null | cut -c1-110 | sed 's/^/  /'
echo
echo "=== 编译/测试进程 ==="
pgrep -af 'cargo|rustc' | head -3 | cut -c1-100 | sed 's/^/  /' || echo "  无"
