#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/artifacts/bin/brewfs-patched
NS=c4ws
PD=http://10.20.0.12:2379
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
echo "=== 配置检查 ==="
ls -l /srv/ax-data/c4ws/data.yaml 2>/dev/null | sed 's/^/  /'
cat /srv/ax-data/c4ws/data.yaml 2>/dev/null | sed 's/^/  /'
echo
echo "=== init-volume（前台，捕获错误）==="
timeout 90 $B workspace init-volume --meta-backend tikv --meta-tikv-pd-endpoints $PD \
  --workspace-namespace $NS --data-config /srv/ax-data/c4ws/data.yaml --owner c4 2>&1 | tail -15 | sed 's/^/  /'
echo "  rc=$?"
EOS
