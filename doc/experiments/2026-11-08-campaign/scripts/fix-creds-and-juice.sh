#!/bin/bash
# 修：把真实凭据送到 obj 并用它重启 RustFS；随后重建 JuiceFS 卷
set -u
K=/root/.ssh/ax-c1-20261106
CLIENT=10.20.2.161
OBJ=10.20.2.163

echo "=== 1. 取 client 凭据 ==="
CREDS=$(ssh -o StrictHostKeyChecking=no -i $K root@$CLIENT 'echo "$(cat /etc/ax-c1/rustfs-access):$(cat /etc/ax-c1/rustfs-secret)"')
AK=${CREDS%%:*}; SK=${CREDS##*:}
echo "  access=${AK:0:6}… secret=${SK:0:6}…"

echo "=== 2. obj 上用真实凭据重启 RustFS ==="
ssh -o StrictHostKeyChecking=no -i $K root@$OBJ "bash -s" <<EOS 2>&1 | sed 's/^/  /'
set -u
pkill -x rustfs 2>/dev/null; sleep 3
export RUSTFS_ACCESS_KEY='$AK'
export RUSTFS_SECRET_KEY='$SK'
setsid nohup /srv/ax-data/bin/rustfs --address 0.0.0.0:9000 --console-address 0.0.0.0:9001 \
  /srv/ax-data/rustfs </dev/null >>/srv/ax-data/logs/rustfs.log 2>&1 &
for i in $(seq 1 30); do sleep 1; code=\$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:9000/health || true); [ "\$code" = "200" ] && break; done
echo "  health=\$code pid=\$(pgrep -x rustfs | head -1)"
tr '\0' '\n' < /proc/\$(pgrep -x rustfs | head -1)/environ | grep -E 'RUSTFS_(ACCESS|SECRET)' | sed -E 's/(=.{6}).*/\1…/' | sed 's/^/  /'
EOS

echo "=== 3. 重建 JuiceFS 卷 ==="
ssh -o StrictHostKeyChecking=no -i $K root@$CLIENT 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
J=/srv/ax-data/artifacts/bin/juicefs
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
$J format --storage s3 --bucket http://10.20.2.163:9000/c1v2-juice \
  --access-key "$AK" --secret-key "$SK" --trash-days 0 \
  tikv://10.20.2.162:2379/c1fio2 c1fio2 2>&1 | tail -2
mkdir -p /srv/ax-data/jcache /mnt/jfs-c1v2
pkill -f 'juicefs mount.*jfs-c1v2' 2>/dev/null; umount -l /mnt/jfs-c1v2 2>/dev/null || true
$J mount -d --cache-size 102400 --cache-dir /srv/ax-data/jcache \
  tikv://10.20.2.162:2379/c1fio2 /mnt/jfs-c1v2 > /srv/ax-data/logs/juice-mount.log 2>&1
for i in $(seq 1 30); do sleep 1; mountpoint -q /mnt/jfs-c1v2 && break; done
echo "  juicefs mount=$(mountpoint -q /mnt/jfs-c1v2 && echo y || echo n)"
echo hello > /mnt/jfs-c1v2/t.txt 2>&1 && echo "  write OK: $(cat /mnt/jfs-c1v2/t.txt)" || echo "  write FAIL"
EOS
