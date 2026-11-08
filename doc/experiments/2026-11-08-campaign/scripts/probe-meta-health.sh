#!/bin/bash
# 判定 TiKV 元数据层是否健康：格式化一个新卷，能否正常 status / 读写
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
J=/srv/ax-data/artifacts/bin/juicefs
V=probe-20261107
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
echo "=== 格式化探针卷 $V ==="
$J format --storage s3 --bucket http://10.20.0.13:9000/probe-20261107 \
  --access-key "$AK" --secret-key "$SK" tikv://10.20.0.12:2379/probe20261107 $V 2>&1 | tail -3 | sed 's/^/  /'
echo
echo "=== status ==="
$J status tikv://10.20.0.12:2379/probe20261107 2>&1 | grep -aE 'Volume|used|inodes|TrashDays|not formatted' | head -4 | sed 's/^/  /'
echo
echo "=== 挂载并写一个文件 ==="
mkdir -p /mnt/probe
$J mount -d tikv://10.20.0.12:2379/probe20261107 /mnt/probe >/dev/null 2>&1
for i in $(seq 1 20); do sleep 1; mountpoint -q /mnt/probe && break; done
echo "  mount: $(mountpoint -q /mnt/probe && echo y || echo n)"
echo hello > /mnt/probe/t.txt 2>/dev/null && echo "  write OK: $(cat /mnt/probe/t.txt 2>/dev/null)" || echo "  write FAIL"
$J umount /mnt/probe >/dev/null 2>&1 || umount -l /mnt/probe 2>/dev/null || true
echo
echo "=== 再 status（确认可重复访问）==="
$J status tikv://10.20.0.12:2379/probe20261107 2>&1 | grep -aE 'used|inodes|not formatted' | head -3 | sed 's/^/  /'
EOS
