#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== replay 输出（stdout/stderr）==="
cat /srv/ax-data/c2b2/brew-pytest-full-02.out 2>/dev/null | head -20 | cut -c1-160 | sed 's/^/  /'
echo
echo "=== 历史 juicefs 版命令（strict-pytest2.sh）==="
cat /srv/ax-data/c2/strict-pytest2.sh 2>/dev/null | head -20 | sed 's/^/  /'
echo
echo "=== 历史输出对照 ==="
head -6 /srv/ax-data/c2/strict-pytest6.out 2>/dev/null | cut -c1-140 | sed 's/^/  /'
EOS
