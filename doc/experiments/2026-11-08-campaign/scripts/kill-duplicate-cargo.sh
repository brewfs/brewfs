#!/bin/bash
set -u
echo "=== 1. 杀掉等锁的 rebuild（pid 1122564 及其子）==="
kill -9 1122564 2>/dev/null && echo "  已杀 1122564" || echo "  （已不存在）"
echo
echo "=== 2. 保留唯一的编译者 ==="
ps -eo pid,ppid,etime,pcpu,rss,args --sort=-rss 2>/dev/null | grep -E 'cargo|rustc' | grep -v grep | head -6 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== 3. 等 2 分钟看进展 ==="
sleep 120
echo "--- rustc 状态 ---"
ps -C rustc -o etime,pcpu,rss --no-headers 2>/dev/null | head -2 | awk '{printf "  etime=%s cpu=%s%% rss=%.1fGB\n",$1,$2,$3/1048576}'
echo "--- final.log 尾 10 行 ---"
tail -10 /mnt/brewfs-data/home/hxy/issue424/final.log 2>/dev/null | cut -c1-130 | sed 's/^/  /'
echo "--- 日志大小 ---"
stat -c '  %s B' /mnt/brewfs-data/home/hxy/issue424/final.log 2>/dev/null
