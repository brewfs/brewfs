#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 脚本目录 ==="
ls -1 /srv/ax-data/src/scripts/ 2>/dev/null | head -20 | sed 's/^/  /'
echo
echo "=== full-fuse-replay --help ==="
timeout 20 /srv/ax-data/artifacts/bin/full-fuse-replay --help 2>&1 | head -30 | sed 's/^/  /'
echo
echo "=== traces 结构 ==="
ls /srv/ax-data/traces/extracted/pytest-full-02 2>/dev/null | head -6 | sed 's/^/  /'
ls /srv/ax-data/traces/extracted/django 2>/dev/null | head -6 | sed 's/^/  /'
echo
echo "=== 历史 C2 运行目录 ==="
ls -1d /srv/ax-data/c2* /srv/ax-data/*c2* 2>/dev/null | head -8 | sed 's/^/  /'
EOS
