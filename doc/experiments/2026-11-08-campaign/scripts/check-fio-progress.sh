#!/bin/bash
# 看 fio 是否真在推进：目标目录文件大小增长
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
D=/mnt/juicefs-c1/c1-data/seq-1m
echo "=== 采样 1 ==="
du -sh $D 2>/dev/null | sed 's/^/  /'
ls -l $D 2>/dev/null | head -4 | awk '{print "  "$5" "$9}'
sleep 20
echo "=== 采样 2（20 s 后）==="
du -sh $D 2>/dev/null | sed 's/^/  /'
echo
echo "=== fio 进程耗时/状态 ==="
ps -o pid,etime,pcpu,stat,args -p $(pgrep -d, -f 'fio --name=juicefs' 2>/dev/null) 2>/dev/null | head -3 | cut -c1-110 | sed 's/^/  /'
echo
echo "=== fio 输出文件 ==="
ls -lt /srv/ax-data/c1c2/c1/logs 2>/dev/null | head -4 | awk '{print "  "$6" "$7" "$8"  "$9}'
tail -3 /srv/ax-data/c1c2/c1/logs/fio-juicefs-1m-write*.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
EOS
