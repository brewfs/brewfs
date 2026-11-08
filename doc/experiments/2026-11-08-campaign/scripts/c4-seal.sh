#!/bin/bash
# 收尾：停生成 → 卸载（seal）→ 计时 fork（count=1 / count=100）→ 验证隔离
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1

pkill -9 -f gen100k.py 2>/dev/null || true
sleep 1
N=$(find /mnt/c4ws/base -type f 2>/dev/null | wc -l)
echo "=== 已写入文件数: $N ==="
echo
echo "=== 卸载（触发 seal）==="
T0=$(date +%s%N)
umount /mnt/c4ws 2>&1 | sed 's/^/  /'
T1=$(date +%s%N)
echo "  umount 耗时: $(( (T1-T0)/1000000 )) ms  mountpoint=$(mountpoint -q /mnt/c4ws && echo 仍挂载 || echo 已卸载)"
echo
echo "=== list（看 sealed_version）==="
timeout 120 $B workspace list --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS 2>&1 | grep -aE 'workspace_id|sealed_version|head_layer_id' | head -12 | sed 's/^/  /'
EOS
