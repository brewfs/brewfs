#!/bin/bash
# 把二进制分发到三台新节点
set -u
K=/root/.ssh/ax-c1-20261106
CLIENT=10.20.2.161
META=10.20.2.162
OBJ=10.20.2.163

echo "=== 创建目录 ==="
for ip in $CLIENT $META $OBJ; do
  ssh -o StrictHostKeyChecking=no -i $K root@$ip 'mkdir -p /srv/ax-data/bin /srv/ax-data/logs /etc/ax-c1' 2>/dev/null
done

echo "=== meta: pd/tikv ==="
scp -q -o StrictHostKeyChecking=no -i $K /root/ax-bin/pd-server /root/ax-bin/tikv-server root@$META:/srv/ax-data/bin/ && echo "  ok"
ssh -o StrictHostKeyChecking=no -i $K root@$META 'chmod +x /srv/ax-data/bin/*; ls -lh /srv/ax-data/bin | tail -3' 2>&1 | sed 's/^/  /'

echo "=== obj: rustfs ==="
scp -q -o StrictHostKeyChecking=no -i $K /root/ax-bin/rustfs root@$OBJ:/srv/ax-data/bin/ && echo "  ok"
ssh -o StrictHostKeyChecking=no -i $K root@$OBJ 'chmod +x /srv/ax-data/bin/rustfs; /srv/ax-data/bin/rustfs --version 2>&1 | head -2' 2>&1 | sed 's/^/  /'

echo "=== client: juicefs/brewfs/smallfiles/replay ==="
scp -q -o StrictHostKeyChecking=no -i $K /root/dist.tar.gz root@$CLIENT:/root/ && echo "  dist 已传"
ssh -o StrictHostKeyChecking=no -i $K root@$CLIENT 'mkdir -p /srv/ax-data/artifacts/bin && tar -C /srv/ax-data/artifacts/bin -xzf /root/dist.tar.gz && chmod +x /srv/ax-data/artifacts/bin/* 2>/dev/null; ls /srv/ax-data/artifacts/bin' 2>&1 | sed 's/^/  /'

echo "=== 复制凭据 ==="
for ip in $CLIENT $OBJ; do
  ssh -o StrictHostKeyChecking=no -i $K root@$ip 'tar -C /tmp -xzf /root/dist.tar.gz rustfs-access rustfs-secret 2>/dev/null || true' 2>/dev/null
done
ssh -o StrictHostKeyChecking=no -i $K root@$CLIENT 'cd /srv/ax-data/artifacts/bin && cp rustfs-access rustfs-secret /etc/ax-c1/ 2>/dev/null; ls /etc/ax-c1' 2>&1 | sed 's/^/  /'
