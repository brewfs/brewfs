#!/bin/bash
# C4：挂载 workspace-v1 卷 → 写 10 万文件 → 卸载 seal → fork 计时
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1

echo "=== mount --volume-format workspace-v1 相关参数 ==="
$B mount --help 2>&1 | grep -E '^\s+--(config|volume-format|workspace|meta|data)' | head -12 | sed 's/^/  /'
echo
echo "=== 尝试挂载（用 c3 配置模板 + workspace 格式）==="
mkdir -p /mnt/c4ws
pkill -f 'brewfs mount.*c4ws' 2>/dev/null; umount -l /mnt/c4ws 2>/dev/null || true
timeout 60 $B mount --help 2>&1 | head -5 | sed 's/^/  /'
echo
echo "=== 直接试：mount --volume-format workspace-v1（凭据走环境变量）==="
setsid nohup $B mount --privileged --volume-format workspace-v1 \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS \
  --data-config /srv/ax-data/c4ws/data.yaml /mnt/c4ws > /srv/ax-data/c4ws/mount.log 2>&1 &
for i in $(seq 1 30); do sleep 1; mountpoint -q /mnt/c4ws && break; done
echo "  mountpoint: $(mountpoint -q /mnt/c4ws && echo y || echo n)"
tail -6 /srv/ax-data/c4ws/mount.log 2>/dev/null | cut -c1-140 | sed 's/^/  /'
EOS
