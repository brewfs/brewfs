#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
A=$(find /mnt/c4ws/base -type f 2>/dev/null | wc -l); sleep 20; B=$(find /mnt/c4ws/base -type f 2>/dev/null | wc -l)
echo "  文件数: $A -> $B（20 秒，速率 $(( (B-A)/20 )) files/s）"
echo "  gen 进程: $(pgrep -fc gen100k.py || echo 0)"
tail -3 /srv/ax-data/c4ws/gen100k.log 2>/dev/null | sed 's/^/  /'
echo "  磁盘: $(df -h /srv/ax-data | tail -1 | awk '{print $3"/"$2}')"
EOS
