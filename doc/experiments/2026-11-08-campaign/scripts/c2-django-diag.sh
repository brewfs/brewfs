#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== console ==="
cat /srv/ax-data/c2b2-django-console.log 2>/dev/null | head -20 | cut -c1-170 | sed 's/^/  /'
echo "=== 输出文件 ==="
ls -l /srv/ax-data/c2b2/brew-django* 2>/dev/null | sed 's/^/  /'
echo "=== replay 进程 ==="
echo "  full-fuse-replay: $(pgrep -fc full-fuse-replay || echo 0)"
EOS
