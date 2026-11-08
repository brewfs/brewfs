#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== gc1j 当前写入目标 ==="
for d in /mnt/juicefs-c1/c1-data/seq-1m /srv/ax-data/c1j/raw; do
  echo "  [$d]"
  du -sh "$d" 2>/dev/null | sed 's/^/    /'
done
ls -lt /mnt/juicefs-c1/c1-data/ 2>/dev/null | head -4 | sed 's/^/  /'
sleep 15
echo "=== 15 s 后 ==="
du -sh /mnt/juicefs-c1/c1-data/seq-1m 2>/dev/null | sed 's/^/  /'
echo
echo "=== fio 进程 ==="
ps -o pid,etime,pcpu,stat,args -p $(pgrep -d, -f 'fio --name' 2>/dev/null) 2>/dev/null | head -3 | cut -c1-130 | sed 's/^/  /'
echo
echo "=== c1j 目录 ==="
ls -A /srv/ax-data/c1j 2>/dev/null | tr '\n' ' ' | sed 's/^/  /'
echo
echo "=== juicefs 客户端统计（会话/缓存）==="
cat /sys/fs/juicefs/*/stats 2>/dev/null | head -8 | sed 's/^/  /'
EOS
