#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
echo "=== 配置 ==="
cat /srv/ax-data/c4ws/data.yaml 2>/dev/null | sed 's/^/  /'
echo
echo "=== init-volume（完整输出）==="
timeout 120 $B workspace init-volume --meta-backend tikv --meta-tikv-pd-endpoints $PD \
  --workspace-namespace $NS --data-config /srv/ax-data/c4ws/data.yaml --owner c4 2>&1 | tail -20 | sed 's/^/  /'
echo "  rc=$?"
EOS
