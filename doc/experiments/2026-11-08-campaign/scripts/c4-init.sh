#!/bin/bash
# C4 正式实验（小规模先行）：建 workspace-v1 卷 → 写入 10k 文件 → 看 revision → fork N 个 → 计时
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/artifacts/bin/brewfs-patched
NS=c4ws
PD=http://10.20.0.12:2379
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
mkdir -p /srv/ax-data/c4ws
DATA_CFG=/srv/ax-data/c4ws/data.yaml
cat > $DATA_CFG <<YAML
data:
  bucket: c4ws-20261107
  endpoint: "http://10.20.0.13:9000"
  region: us-east-1
YAML
# 建桶目录
ssh -o StrictHostKeyChecking=no -i /root/.ssh/ax-c1-20261106 root@10.20.0.13 'mkdir -p /srv/ax-data/rustfs/c4ws-20261107' 2>/dev/null

echo "=== 1. init-volume ==="
timeout 120 $B workspace init-volume --meta-backend tikv --meta-tikv-pd-endpoints $PD \
  --workspace-namespace $NS --data-config $DATA_CFG --owner c4 2>&1 | tail -8 | sed 's/^/  /'

echo
echo "=== 2. list ==="
timeout 60 $B workspace list --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS 2>&1 | head -20 | sed 's/^/  /'
EOS
