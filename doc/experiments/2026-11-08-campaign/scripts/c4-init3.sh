#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
mkdir -p /srv/ax-data/c4ws
cat > /srv/ax-data/c4ws/data.yaml <<YAML
data:
  bucket: c4ws-20261108
  endpoint: "http://10.20.0.13:9000"
  region: us-east-1
YAML
ssh -o StrictHostKeyChecking=no -i /root/.ssh/ax-c1-20261106 root@10.20.0.13 'mkdir -p /srv/ax-data/rustfs/c4ws-20261108' 2>/dev/null

echo "=== 1. init-volume ==="
timeout 180 $B workspace init-volume --meta-backend tikv --meta-tikv-pd-endpoints $PD \
  --workspace-namespace $NS --data-config /srv/ax-data/c4ws/data.yaml --owner c4 2>&1 | tail -6 | sed 's/^/  /'
echo "  rc=$?"
echo
echo "=== 2. list ==="
timeout 90 $B workspace list --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS 2>&1 | tail -12 | sed 's/^/  /'
echo
echo "=== 3. mount 帮助里的 workspace 参数 ==="
$B mount --help 2>&1 | grep -iE 'workspace|format|volume' | head -10 | sed 's/^/  /'
EOS
