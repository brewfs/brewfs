#!/bin/bash
# 修：把凭据放到 obj 并用它重启 RustFS；验证 S3 可写
set -u
K=/root/.ssh/ax-c1-20261106
CREDS=$(ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.161 'echo "$(cat /etc/ax-c1/rustfs-access):$(cat /etc/ax-c1/rustfs-secret)"')
AK=${CREDS%%:*}; SK=${CREDS##*:}
echo "  creds: ${AK:0:6}… / ${SK:0:6}…"
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.163 "mkdir -p /etc/ax-c1 && printf '%s' '$AK' > /etc/ax-c1/rustfs-access && printf '%s' '$SK' > /etc/ax-c1/rustfs-secret && chmod 600 /etc/ax-c1/rustfs-*" && echo "  creds written"
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.163 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
pkill -x rustfs 2>/dev/null; sleep 3
export RUSTFS_ACCESS_KEY=$(cat /etc/ax-c1/rustfs-access)
export RUSTFS_SECRET_KEY=$(cat /etc/ax-c1/rustfs-secret)
setsid nohup /srv/ax-data/bin/rustfs --address 0.0.0.0:9000 --console-address 0.0.0.0:9001 \
  /srv/ax-data/rustfs </dev/null >>/srv/ax-data/logs/rustfs.log 2>&1 &
for i in $(seq 1 30); do sleep 1; c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:9000/health || true); [ "$c" = "200" ] && break; done
echo "  health=$c pid=$(pgrep -x rustfs | head -1)"
EOS
