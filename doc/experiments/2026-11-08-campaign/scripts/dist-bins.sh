#!/bin/bash
# 把可复用二进制与配置传到 jump 暂存，再由 jump 分发到 a-meta/a-obj（不重编译）
set -u
K=/root/.ssh/ax-c1-20261106
C=10.20.0.11
echo "=== 1. a-client 打包（只打需要的）==="
ssh -i $K -o StrictHostKeyChecking=no root@$C 'bash -s' <<'EOS' 2>&1
set -u
mkdir -p /srv/ax-data/artifacts/dist
cd /srv/ax-data/artifacts/bin
for f in brewfs-patched smallfiles full-fuse-replay juicefs; do
  cp -f "$f" /srv/ax-data/artifacts/dist/ 2>/dev/null || true
done
cp -f /etc/ax-c1/brewfs-c1.yaml /srv/ax-data/artifacts/dist/ 2>/dev/null || true
cp -f /etc/ax-c1/rustfs-access /etc/ax-c1/rustfs-secret /srv/ax-data/artifacts/dist/ 2>/dev/null || true
tar -C /srv/ax-data/artifacts/dist -czf /srv/ax-data/artifacts/dist.tar.gz . 2>/dev/null
ls -lh /srv/ax-data/artifacts/dist.tar.gz | awk '{print "  "$5" "$9}'
md5sum /srv/ax-data/artifacts/dist.tar.gz | sed 's/^/  /'
EOS

echo
echo "=== 2. 拉到 jump ==="
scp -q -o StrictHostKeyChecking=no -i $K root@$C:/srv/ax-data/artifacts/dist.tar.gz /root/dist.tar.gz && echo "  ok $(du -h /root/dist.tar.gz | cut -f1)"

echo
echo "=== 3. 分发到 a-meta / a-obj ==="
for h in 10.20.0.12 10.20.0.13; do
  scp -q -o StrictHostKeyChecking=no -i $K /root/dist.tar.gz root@$h:/root/dist.tar.gz 2>/dev/null && \
  ssh -i $K -o StrictHostKeyChecking=no root@$h 'mkdir -p /srv/ax-data/artifacts/bin && tar -xzf /root/dist.tar.gz -C /srv/ax-data/artifacts/bin && chmod +x /srv/ax-data/artifacts/bin/{brewfs-patched,smallfiles,full-fuse-replay,juicefs} 2>/dev/null; ls -1 /srv/ax-data/artifacts/bin | tr "\n" " "' 2>/dev/null | sed "s/^/  $h: /"
done

echo
echo "=== 4. 磁盘 ==="
for h in 10.20.0.12 10.20.0.13; do
  ssh -i $K -o StrictHostKeyChecking=no root@$h "df -h /srv/ax-data 2>/dev/null | tail -1 | sed 's/^/  $h /'" 2>/dev/null
done
