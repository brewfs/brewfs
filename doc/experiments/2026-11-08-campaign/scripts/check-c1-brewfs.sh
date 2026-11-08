#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== BrewFS c1 写入进度 ==="
du -sh /mnt/brewfs-c1/c1-data/seq-1m 2>/dev/null | sed 's/^/  /'
ls -l /mnt/brewfs-c1/c1-data/seq-1m 2>/dev/null | head -4 | awk '{print "  "$5" "$9}'
sleep 20
echo "=== 20 s 后 ==="
du -sh /mnt/brewfs-c1/c1-data/seq-1m 2>/dev/null | sed 's/^/  /'
echo
echo "=== fio 进程 & 目标 ==="
ps -o pid,etime,pcpu,stat,args -p $(pgrep -d, -f 'fio --name' 2>/dev/null) 2>/dev/null | head -3 | cut -c1-120 | sed 's/^/  /'
echo
echo "=== brewfs RSS/CPU ==="
ps -C brewfs-patched -o pid,pcpu,rss,etime --no-headers 2>/dev/null | awk '{printf "  pid=%s cpu=%s%% rss=%.0fMB etime=%s\n", $1, $2, $3/1024, $4}'
echo
echo "=== rustfs 侧（a-obj 无法直连，看 brewfs 日志尾）==="
ls -lt /srv/ax-data/c1c2/c1/logs 2>/dev/null | head -3 | awk '{print "  "$6" "$7" "$8"  "$9}'
EOS
