#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 当前挂载 ==="
mount | grep juicefs | cut -c1-120 | sed 's/^/  /'
echo "=== 手动 umount + mount ==="
juicefs umount /mnt/juicefs-c1 2>&1 | head -3 | sed 's/^/  umount: /'
sleep 2
mountpoint -q /mnt/juicefs-c1 && echo "  仍挂载" || echo "  已卸载"
J=/srv/ax-data/artifacts/bin/juicefs
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK"
timeout 60 $J mount -d --enable-xattr --enable-cap \
  --cache-dir=memory --cache-size=512 --buffer-size=512 --writeback=false --cache-large-write=true \
  tikv://10.20.0.12:2379/c1b2 /mnt/juicefs-c1 2>&1 | tail -5 | sed 's/^/  mount: /'
sleep 3
echo "  mountpoint: $(mountpoint -q /mnt/juicefs-c1 && echo y || echo n)"
echo "  juicefs 进程: $(pgrep -fc 'juicefs mount' || echo 0)"
ls -ld /mnt/juicefs-c1 2>/dev/null | sed 's/^/  /'
timeout 20 bash -c 'echo hi > /mnt/juicefs-c1/t.txt' && echo "  write: OK" || echo "  write: FAIL"
cat /mnt/juicefs-c1/t.txt 2>/dev/null | sed 's/^/  read: /'
rm -f /mnt/juicefs-c1/t.txt
EOS
