#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
scp -q -o StrictHostKeyChecking=no -i $K /root/c2-django.sh root@10.20.0.11:/root/c2-django.sh 2>/dev/null || true
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
pkill -f 'c2-django.sh' 2>/dev/null || true
setsid nohup bash /root/c2-django.sh > /srv/ax-data/c2b2-django-console.log 2>&1 &
sleep 20
echo "  c2-django 进程: $(pgrep -fc c2-django.sh || echo 0)"
tail -3 /srv/ax-data/c2b2/brew-django.out 2>/dev/null | cut -c1-150 | sed 's/^/  /'
EOS
