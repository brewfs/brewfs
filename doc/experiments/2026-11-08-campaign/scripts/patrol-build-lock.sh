#!/bin/bash
set -u
echo "=== 编译相关进程 CPU ==="
ps -eo pid,etime,pcpu,rss,args --sort=-pcpu | grep -E 'cargo|rustc' | grep -v grep | head -5 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== cargo 进程详情 ==="
ps -o pid,etime,stat,pcpu,args -p 2156898 2>/dev/null | tail -2 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== target 目录更新时间 ==="
ls -lt /home/hxy/brewfs-main/target/release/ 2>/dev/null | head -3 | awk '{print "  "$6" "$7" "$8"  "$9}'
echo
echo "=== 是否有文件锁等待（cargo 输出） ==="
grep -a 'Blocking waiting' /home/hxy/gxattr2.log 2>/dev/null | tail -2 | sed 's/^/  /' || echo "  无锁等待记录"
