#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 历史 replay 命令（c2 目录脚本）==="
cat /srv/ax-data/c2/run-django-long.sh 2>/dev/null | head -20 | sed 's/^/  /'
echo
echo "=== C2 目录里的 driver / 报告结构 ==="
ls -1 /srv/ax-data/c2b 2>/dev/null | head -20 | sed 's/^/  /'
echo
echo "=== pytest 历史输出（c2 目录）==="
ls -1 /srv/ax-data/c2/*pytest* 2>/dev/null | sed 's/^/  /'
echo
echo "=== full_trace_common.py 里的 replay 调用 ==="
grep -n 'full-fuse-replay' -A4 -B2 /srv/ax-data/src/scripts/full_trace_common.py 2>/dev/null | head -20 | cut -c1-160 | sed 's/^/  /'
EOS
