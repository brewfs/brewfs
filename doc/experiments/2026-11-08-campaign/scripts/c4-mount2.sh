#!/bin/bash
# 用正确参数挂载 workspace-v1（data-backend s3 + s3-* 参数 + --workspace <id>）
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
WID=01a1b6e7-db96-7fb2-ae2a-69cf7851bec6
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
echo "=== 可用 s3 参数 ==="
$B mount --help 2>&1 | grep -E '^\s+--s3' | head -8 | sed 's/^/  /'
echo
mkdir -p /mnt/c4ws
pkill -f 'brewfs mount.*c4ws' 2>/dev/null || true
umount -l /mnt/c4ws 2>/dev/null || true
echo "=== 挂载 ==="
setsid nohup $B mount --privileged --volume-format workspace-v1 \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS \
  --workspace $WID \
  --data-backend s3 --s3-bucket c4ws-20261108 \
  --s3-endpoint http://10.20.0.13:9000 --s3-region us-east-1 \
  /mnt/c4ws > /srv/ax-data/c4ws/mount.log 2>&1 &
for i in $(seq 1 40); do sleep 1; mountpoint -q /mnt/c4ws && break; done
echo "  mountpoint: $(mountpoint -q /mnt/c4ws && echo y || echo n)"
tail -8 /srv/ax-data/c4ws/mount.log 2>/dev/null | cut -c1-150 | sed 's/^/  /'
EOS
