#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.12 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 二进制存在性 ==="
ls -l /srv/ax-data/bin/tikv-server /srv/ax-data/bin/pd-server 2>&1 | sed 's/^/  /'
echo
echo "=== 进程 ==="
pgrep -af 'tikv-server|pd-server' | cut -c1-90 | sed 's/^/  /'
echo
echo "=== 端口 ==="
for p in 2379 20160; do
  timeout 2 bash -c "</dev/tcp/127.0.0.1/$p" 2>/dev/null && echo "  $p up" || echo "  $p down"
done
echo
echo "=== TiKV 日志尾 ==="
tail -5 /srv/ax-data/logs/tikv-stdout.log 2>/dev/null | cut -c1-130 | sed 's/^/  /'
EOS
