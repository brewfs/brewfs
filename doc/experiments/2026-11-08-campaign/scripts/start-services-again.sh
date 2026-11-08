#!/bin/bash
# 服务起来 + 精确测「fork → 可用」端到端延迟
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== PD/TiKV ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.12 'bash -s' <<'EOS' 2>&1
set -u
pkill -f pd-server 2>/dev/null; pkill -f tikv-server 2>/dev/null; sleep 2
setsid nohup /srv/ax-data/bin/pd-server --name=pd-a --data-dir=/srv/ax-data/pd \
  --client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://10.20.0.12:2379 \
  --peer-urls=http://0.0.0.0:2380 --advertise-peer-urls=http://10.20.0.12:2380 \
  --initial-cluster=pd-a=http://10.20.0.12:2380 </dev/null >>/srv/ax-data/logs/pd-stdout.log 2>&1 &
sleep 12
setsid nohup /srv/ax-data/bin/tikv-server --addr=0.0.0.0:20160 --advertise-addr=10.20.0.12:20160 \
  --status-addr=0.0.0.0:20180 --data-dir=/srv/ax-data/tikv \
  --pd-endpoints=10.20.0.12:2379 -C /srv/ax-data/tikv/last_tikv.toml </dev/null >>/srv/ax-data/logs/tikv-stdout.log 2>&1 &
sleep 25
echo "  PD=$(pgrep -c pd-server||echo 0) TiKV=$(pgrep -c tikv-server||echo 0)"
EOS
echo "=== RustFS ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.13 'bash -s' <<'EOS' 2>&1
set -u
pkill -f rustfs 2>/dev/null; sleep 2
export RUSTFS_ACCESS_KEY=$(cat /etc/ax-c1/rustfs-access); export RUSTFS_SECRET_KEY=$(cat /etc/ax-c1/rustfs-secret)
setsid nohup /srv/ax-data/bin/rustfs --address 0.0.0.0:9000 --console-address 0.0.0.0:9001 /srv/ax-data/rustfs </dev/null >>/srv/ax-data/logs/rustfs.log 2>&1 &
sleep 8
echo "  RustFS=$(pgrep -c rustfs||echo 0) health=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:9000/health)"
EOS
