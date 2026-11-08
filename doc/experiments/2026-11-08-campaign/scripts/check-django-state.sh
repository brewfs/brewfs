#!/bin/bash
set -u
echo "=== replay 进程 ==="
ps -eo pid,etime,pcpu,rss,args | grep -E 'full-fuse-replay|c2-django-perf' | grep -v grep | cut -c1-130 | sed 's/^/  /'
echo "=== 输出文件 ==="
ls -l /srv/ax-data/c2b2/perf-django/ 2>/dev/null | sed 's/^/  /'
echo "=== console ==="
tail -5 /srv/ax-data/c2b2/django-perf-console.log 2>/dev/null | cut -c1-140 | sed 's/^/  /'
echo "=== 挂载与磁盘 ==="
echo "  mount=$(mountpoint -q /mnt/bfs-c2perf && echo y || echo n)  df=$(df -h /srv/ax-data | tail -1 | awk '{print $3"/"$2" ("$5")"}')"
