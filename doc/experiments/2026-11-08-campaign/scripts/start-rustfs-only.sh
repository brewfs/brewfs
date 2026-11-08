#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.13 'bash -s' <<'EOS' 2>&1
set -u
pkill -x rustfs 2>/dev/null || true
sleep 2
export RUSTFS_ACCESS_KEY=$(cat /etc/ax-c1/rustfs-access)
export RUSTFS_SECRET_KEY=$(cat /etc/ax-c1/rustfs-secret)
setsid nohup /srv/ax-data/bin/rustfs --address 0.0.0.0:9000 --console-address 0.0.0.0:9001 \
  /srv/ax-data/rustfs </dev/null >>/srv/ax-data/logs/rustfs.log 2>&1 &
for i in $(seq 1 30); do
  sleep 1
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:9000/health || true)
  [ "$code" = "200" ] && { echo "  rustfs healthy after ${i}s"; break; }
done
echo "  rustfs=$(pgrep -c rustfs||echo 0)"
EOS
