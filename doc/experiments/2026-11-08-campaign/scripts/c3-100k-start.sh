#!/bin/bash
# 后台跑 C3 100k pilot ×3，日志落盘，便于轮询
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
pkill -f c3-100k.sh 2>/dev/null || true
sleep 1
setsid nohup bash /root/c3-100k.sh > /srv/ax-data/c3-100k-console.log 2>&1 &
sleep 10
echo "  进程: $(pgrep -fc c3-100k.sh || echo 0)"
tail -3 /srv/ax-data/c3-100k-console.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
EOS
