#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== c2b2-django-console.log ==="
cat /srv/ax-data/c2b2-django-console.log 2>/dev/null | tail -20 | cut -c1-170 | sed 's/^/  /'
echo "=== c2b2 目录 ==="
ls -l /srv/ax-data/c2b2/ 2>/dev/null | tail -6 | sed 's/^/  /'
echo "=== 脚本存在? ==="
ls -l /root/c2-django.sh 2>/dev/null | sed 's/^/  /'
EOS
