#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "  full-fuse-replay 进程: $(pgrep -fc full-fuse-replay || echo 0)"
echo "  brew-django.out: $(ls -l /srv/ax-data/c2b2/brew-django.out 2>/dev/null | awk '{print $5" bytes"}')"
tail -3 /srv/ax-data/c2b2/brew-django.out 2>/dev/null | cut -c1-150 | sed 's/^/  /'
echo "  root 文件数: $(find /mnt/bfs-c1fio/c2-django-01 -type f 2>/dev/null | wc -l)"
EOS
