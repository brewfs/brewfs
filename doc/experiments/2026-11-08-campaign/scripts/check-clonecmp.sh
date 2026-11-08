#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== clonecmp2 进程 ==="
pgrep -af 'clonecmp2.py' | head -2 | cut -c1-100 | sed 's/^/  /'
echo "=== 是否在生成文件 ==="
echo "  src 目录数: $(ls /mnt/probe/src 2>/dev/null | wc -l)"
echo "  src 文件数: $(find /mnt/probe/src -type f 2>/dev/null | wc -l)"
echo "=== console ==="
tail -5 /srv/ax-data/clonecmp2-console.log 2>/dev/null | cut -c1-140 | sed 's/^/  /'
echo "=== run.log ==="
cat /srv/ax-data/clonecmp2/run.log 2>/dev/null | sed 's/^/  /'
EOS
