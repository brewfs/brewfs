#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== a-client 卷 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  vol used: $(df -h /mnt/juicefs-c1 2>/dev/null | tail -1 | awk '{print $3}')"
echo "  fio: $(pgrep -fc '/usr/bin/fio' || echo 0)"
echo "  runner: $(pgrep -fc 'c1-run-remaining' || echo 0)"
echo "  remaining.log 尾 3:"
tail -3 /srv/ax-data/c1k/remaining.log 2>/dev/null | cut -c1-110 | sed 's/^/    /'
echo "  metrics: $(ls /srv/ax-data/c1k/meta/*.metrics.json 2>/dev/null | wc -l)"
EOS
echo "=== a-obj 磁盘 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.13 'df -h /srv/ax-data | tail -1 | sed "s/^/  /"' 2>/dev/null
