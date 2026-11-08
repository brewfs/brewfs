#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
pkill -f c3-100k.sh 2>/dev/null || true
pkill -f 'cp -a' 2>/dev/null || true
sleep 2
scp -q -o StrictHostKeyChecking=no -i /root/.ssh/ax-c1-20261106 /root/c3-100k-v2.sh root@10.20.0.11:/root/ 2>/dev/null || true
setsid nohup bash /root/c3-100k-v2.sh > /srv/ax-data/c3-100k-v2.log 2>&1 &
sleep 15
echo "  进程: $(pgrep -fc c3-100k-v2.sh || echo 0)"
tail -4 /srv/ax-data/c3-100k-v2.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
EOS
