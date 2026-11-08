#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
pkill -f c3-reps.sh 2>/dev/null || true
setsid nohup bash /root/c3-reps.sh > /srv/ax-data/c3v-console.log 2>&1 &
sleep 15
echo "  c3 reps 进程: $(pgrep -fc c3-reps.sh || echo 0)"
tail -4 /srv/ax-data/c3v-console.log 2>/dev/null | cut -c1-130 | sed 's/^/  /'
EOS
