#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
J=/srv/ax-data/artifacts/bin/juicefs
echo "=== 卷配置 ==="
$J status tikv://10.20.0.12:2379/c1b2 2>/dev/null | grep -aiE 'trash|used|inodes|sessions' | head -8 | sed 's/^/  /'
echo
echo "=== a-obj 磁盘 ==="
ssh -o StrictHostKeyChecking=no -i /root/.ssh/ax-c1-20261106 root@10.20.0.13 'df -h /srv/ax-data | tail -1' 2>/dev/null | sed 's/^/  /'
echo
echo "=== 当前 fio ==="
pgrep -af '/usr/bin/fio' | head -1 | cut -c1-100 | sed 's/^/  /'
echo "  c1rem 日志尾: $(tail -1 /srv/ax-data/c1k/remaining.log 2>/dev/null | cut -c1-100)"
EOS
