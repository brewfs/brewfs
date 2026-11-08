#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== django 采样进度 ==="
tail -2 /srv/ax-data/c2b2/perf-django/replay.out 2>/dev/null | cut -c1-150 | sed 's/^/  /'
echo "  replay 进程: $(pgrep -fc full-fuse-replay || echo 0)"
echo "  django 脚本: $(pgrep -fc c2-django-perf.sh || echo 0)"
EOS
