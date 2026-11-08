#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
pkill -f c2-django-perf.sh 2>/dev/null || true
setsid nohup bash /root/c2-django-perf.sh > /srv/ax-data/c2b2/django-perf-console.log 2>&1 &
sleep 5
echo "  django 采样已启动（限时 30 分钟）: $(pgrep -fc c2-django-perf.sh || echo 0)"
EOS
