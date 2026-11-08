#!/bin/bash
# 找 revision 格式并首次尝试 fork
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
WID=01a1b6e7-db96-7fb2-ae2a-69cf7851bec6
echo "=== inspect（找 revision 表示）==="
timeout 90 $B workspace inspect --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS $WID 2>&1 | grep -aE 'revision|layer|version|root_hash|head' | head -12 | cut -c1-140 | sed 's/^/  /'
echo
echo "=== fork --help（revision 说明）==="
$B workspace fork --help 2>&1 | sed -n '/REVISION/,+4p' | head -8 | sed 's/^/  /'
echo
echo "=== 尝试 fork（用 layer 01a1b6e7-db96-7fb2-ae2a-69e7d881a4b4）==="
T0=$(date +%s%N)
timeout 120 $B workspace fork 01a1b6e7-db96-7fb2-ae2a-69e7d881a4b4 --count 1 \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS --owner c4f 2>&1 | tail -6 | cut -c1-140 | sed 's/^/  /'
T1=$(date +%s%N)
echo "  fork 耗时: $(( (T1-T0)/1000000 )) ms"
EOS
