#!/bin/bash
# 正确启动：/srv/ax-data/bin 下的 PD/TiKV，用 setsid + nohup，并等待端口
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.12 'bash -s' <<'EOS' 2>&1
set -u
pkill -f pd-server 2>/dev/null; pkill -f tikv-server 2>/dev/null; sleep 3
mkdir -p /srv/ax-data/logs
setsid nohup /srv/ax-data/bin/pd-server --name=pd-a --data-dir=/srv/ax-data/pd \
  --client-urls=http://0.0.0.0:2379 --advertise-client-urls=http://10.20.0.12:2379 \
  --peer-urls=http://0.0.0.0:2380 --advertise-peer-urls=http://10.20.0.12:2380 \
  --initial-cluster=pd-a=http://10.20.0.12:2380 \
  </dev/null >>/srv/ax-data/logs/pd-stdout.log 2>&1 &
for i in $(seq 1 30); do sleep 1; curl -s --max-time 2 http://127.0.0.1:2379/pd/api/v1/stores >/dev/null 2>&1 && break; done
echo "  PD 进程: $(pgrep -c pd-server)  API: $(curl -s -o /dev/null -w '%{http_code}' --max-time 4 http://127.0.0.1:2379/pd/api/v1/stores)"

setsid nohup /srv/ax-data/bin/tikv-server --addr=0.0.0.0:20160 --advertise-addr=10.20.0.12:20160 \
  --status-addr=0.0.0.0:20180 --data-dir=/srv/ax-data/tikv \
  --pd-endpoints=10.20.0.12:2379 -C /srv/ax-data/tikv/last_tikv.toml \
  </dev/null >>/srv/ax-data/logs/tikv-stdout.log 2>&1 &
for i in $(seq 1 40); do sleep 1; pgrep -f tikv-server >/dev/null && break; done
sleep 5
echo "  TiKV 进程: $(pgrep -c tikv-server)"
echo "  stores: $(curl -s --max-time 5 http://127.0.0.1:2379/pd/api/v1/stores | grep -o '\"count\": [0-9]*' | head -1)"
tail -3 /srv/ax-data/logs/tikv-stdout.log 2>/dev/null | cut -c1-120 | sed 's/^/    /'
EOS
