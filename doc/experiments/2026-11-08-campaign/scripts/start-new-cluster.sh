#!/bin/bash
# 启动新集群：meta(PD+TiKV) / obj(RustFS)
set -u
K=/root/.ssh/ax-c1-20261106
META=10.20.2.162
OBJ=10.20.2.163

echo "=== meta: PD + TiKV ==="
ssh -o StrictHostKeyChecking=no -i $K root@$META 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
mkdir -p /srv/ax-data/{pd,tikv,logs}
pkill -x pd-server 2>/dev/null; pkill -x tikv-server 2>/dev/null; sleep 2
setsid nohup /srv/ax-data/bin/pd-server --name=pd-b --data-dir=/srv/ax-data/pd \
  --client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://10.20.2.162:2379 \
  --peer-urls=http://0.0.0.0:2380 --advertise-peer-urls=http://10.20.2.162:2380 \
  --initial-cluster=pd-b=http://10.20.2.162:2380 </dev/null >>/srv/ax-data/logs/pd.log 2>&1 &
sleep 14
setsid nohup /srv/ax-data/bin/tikv-server --addr=0.0.0.0:20160 --advertise-addr=10.20.2.162:20160 \
  --status-addr=0.0.0.0:20180 --data-dir=/srv/ax-data/tikv \
  --pd-endpoints=10.20.2.162:2379 </dev/null >>/srv/ax-data/logs/tikv.log 2>&1 &
for i in $(seq 1 40); do sleep 2; timeout 2 bash -c '</dev/tcp/127.0.0.1/20160' 2>/dev/null && break; done
echo "  PD=$(pgrep -x pd-server | wc -l) TiKV=$(pgrep -x tikv-server | wc -l)"
timeout 3 bash -c '</dev/tcp/127.0.0.1/20160' 2>/dev/null && echo "  tikv port up" || echo "  tikv port DOWN"
EOS

echo "=== obj: RustFS ==="
ssh -o StrictHostKeyChecking=no -i $K root@$OBJ 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
mkdir -p /srv/ax-data/rustfs /srv/ax-data/logs
pkill -x rustfs 2>/dev/null; sleep 2
export RUSTFS_ACCESS_KEY=$(cat /srv/ax-data/artifacts/bin/rustfs-access 2>/dev/null || echo x)
export RUSTFS_SECRET_KEY=$(cat /srv/ax-data/artifacts/bin/rustfs-secret 2>/dev/null || echo y)
if [ "$RUSTFS_ACCESS_KEY" = "x" ]; then
  AK=$(grep -ao 'RUSTFS_ACCESS_KEY[^ ]*' /root/dist.tar.gz 2>/dev/null | head -1)
  echo "  凭据缺失，尝试从 tar 解"
  tar -C /tmp -xzf /root/dist.tar.gz rustfs-access rustfs-secret 2>/dev/null && \
    export RUSTFS_ACCESS_KEY=$(cat /tmp/rustfs-access) && export RUSTFS_SECRET_KEY=$(cat /tmp/rustfs-secret)
fi
setsid nohup /srv/ax-data/bin/rustfs --address 0.0.0.0:9000 --console-address 0.0.0.0:9001 \
  /srv/ax-data/rustfs </dev/null >>/srv/ax-data/logs/rustfs.log 2>&1 &
for i in $(seq 1 30); do sleep 1; code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:9000/health || true); [ "$code" = "200" ] && break; done
echo "  rustfs=$(pgrep -x rustfs | wc -l) health=$code"
mkdir -p /srv/ax-data/rustfs/c1v2-juice /srv/ax-data/rustfs/c1v2-brew
ls /srv/ax-data/rustfs | head -5 | sed 's/^/  bucket: /'
EOS
