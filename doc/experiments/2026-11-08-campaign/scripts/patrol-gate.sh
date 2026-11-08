#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== 门禁/矩阵进度 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  挂载: $(mountpoint -q /mnt/juicefs-c1 && echo y || echo n)"
echo "  fio: $(pgrep -fc '/usr/bin/fio' || echo 0)"
pgrep -af '/usr/bin/fio' | head -2 | cut -c1-120 | sed 's/^/    /'
echo "  c1k 目录: $(ls /srv/ax-data/c1k 2>/dev/null | tr '\n' ' ')"
echo "  c1k raw: $(ls /srv/ax-data/c1k/raw/*.json 2>/dev/null | wc -l)"
ls -lt /srv/ax-data/c1k/raw 2>/dev/null | head -3 | awk '{print "    "$6" "$7" "$8"  "$9}'
echo "  卷用量: $(du -sh /mnt/juicefs-c1 2>/dev/null | cut -f1)"
EOS
echo
echo "=== a-obj 磁盘 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.13 'df -h /srv/ax-data | tail -1 | sed "s/^/  /"' 2>/dev/null
