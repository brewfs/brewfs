#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.12 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 二进制位置 ==="
ls -l /srv/ax-data/bin/pd-server /srv/ax-data/bin/tikv-server 2>/dev/null | awk '{print "  "$5" "$9}'
ls -l /srv/ax-data/artifacts/bin/pd-server /srv/ax-data/artifacts/bin/tikv-server 2>/dev/null | awk '{print "  "$5" "$9}'
echo
echo "=== 数据目录 ==="
ls -d /srv/ax-data/pd /srv/ax-data/tikv 2>/dev/null | sed 's/^/  /'
echo
echo "=== 尝试启动 PD（前台 5 秒看错误）==="
PD=$(command -v pd-server || echo /srv/ax-data/bin/pd-server)
[ -x "$PD" ] || PD=/srv/ax-data/artifacts/bin/pd-server
echo "  使用: $PD"
timeout 6 $PD --name=pd-a --data-dir=/srv/ax-data/pd \
  --client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://10.20.0.12:2379 \
  --peer-urls=http://0.0.0.0:2380 --advertise-peer-urls=http://10.20.0.12:2380 \
  --initial-cluster=pd-a=http://10.20.0.12:2380 2>&1 | head -8 | sed 's/^/    /'
EOS
