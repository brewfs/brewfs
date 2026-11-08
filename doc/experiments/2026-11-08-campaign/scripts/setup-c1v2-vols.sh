#!/bin/bash
# 客户端：建两个新卷并挂载（两侧都用默认缓存）
set -u
K=/root/.ssh/ax-c1-20261106
CLIENT=10.20.2.161
ssh -o StrictHostKeyChecking=no -i $K root@$CLIENT 'bash -s' <<'EOS' 2>&1
set -u
J=/srv/ax-data/artifacts/bin/juicefs
B=/srv/ax-data/artifacts/bin/brewfs-patched
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1

echo "=== 1. JuiceFS 卷 c1fio2（默认缓存）==="
$J format --storage s3 --bucket http://10.20.2.163:9000/c1v2-juice \
  --access-key "$AK" --secret-key "$SK" --trash-days 0 \
  tikv://10.20.2.162:2379/c1fio2 c1fio2 2>&1 | tail -2 | sed 's/^/  /'
mkdir -p /srv/ax-data/jcache /mnt/jfs-c1v2
pkill -f 'juicefs mount.*jfs-c1v2' 2>/dev/null; umount -l /mnt/jfs-c1v2 2>/dev/null || true
$J mount -d --cache-size 102400 --cache-dir /srv/ax-data/jcache \
  tikv://10.20.2.162:2379/c1fio2 /mnt/jfs-c1v2 > /srv/ax-data/logs/juice-mount.log 2>&1
for i in $(seq 1 30); do sleep 1; mountpoint -q /mnt/jfs-c1v2 && break; done
echo "  juicefs mount=$(mountpoint -q /mnt/jfs-c1v2 && echo y || echo n)"

echo "=== 2. BrewFS 配置与挂载（默认缓存）==="
cat > /etc/ax-c1/brewfs-c1v2.yaml <<YAML
mount_point: /mnt/bfs-c1v2
data:
  backend: s3
  s3:
    bucket: c1v2-brew
    endpoint: "http://10.20.2.163:9000"
    region: us-east-1
    part_size: 16777216
    max_concurrency: 8
    force_path_style: true
    disable_payload_checksum: true
meta:
  backend: tikv
  tikv:
    pd_endpoints:
      - "10.20.2.162:2379"
  namespace: c1v2
layout:
  chunk_size: 67108864
  block_size: 4194304
fuse:
  cache_ttl_ms: 0
  workers: 16
YAML
mkdir -p /mnt/bfs-c1v2
pkill -f 'brewfs-patched mount.*bfs-c1v2' 2>/dev/null; sleep 1; umount -l /mnt/bfs-c1v2 2>/dev/null || true
setsid nohup $B mount --privileged --config /etc/ax-c1/brewfs-c1v2.yaml /mnt/bfs-c1v2 \
  > /srv/ax-data/logs/brew-mount.log 2>&1 &
for i in $(seq 1 40); do sleep 1; mountpoint -q /mnt/bfs-c1v2 && break; done
echo "  brewfs mount=$(mountpoint -q /mnt/bfs-c1v2 && echo y || echo n)"
tail -3 /srv/ax-data/logs/brew-mount.log 2>/dev/null | cut -c1-130 | sed 's/^/  /'

echo "=== 3. 写测试 ==="
echo hello > /mnt/jfs-c1v2/t.txt 2>&1 && echo "  juicefs write OK" || echo "  juicefs write FAIL"
echo hello > /mnt/bfs-c1v2/t.txt 2>&1 && echo "  brewfs write OK" || echo "  brewfs write FAIL"
df -h /srv/ax-data | tail -1 | sed 's/^/  disk: /'
EOS
