#!/bin/bash
# 在 TiKV 测试机上装 docker 并起 PD + TiKV 单节点
set -eu
PRIV=172.24.86.193
echo "=== 1. 安装 docker（用阿里云镜像加速）==="
export DEBIAN_FRONTEND=noninteractive
if ! command -v docker >/dev/null 2>&1; then
  apt-get update -qq
  apt-get install -y -qq docker.io >/dev/null 2>&1 || apt-get install -y -qq docker-ce >/dev/null 2>&1 || true
fi
command -v docker >/dev/null 2>&1 && echo "  docker: $(docker --version)" || { echo "  ❌ docker 安装失败"; exit 1; }
systemctl enable --now docker >/dev/null 2>&1 || service docker start >/dev/null 2>&1 || true
sleep 3
docker info >/dev/null 2>&1 && echo "  docker daemon ok" || { echo "  ❌ daemon 未起"; exit 1; }

echo "=== 2. 拉镜像 ==="
for img in pingcap/pd:v7.5.0 pingcap/tikv:v7.5.0; do
  echo "  pulling $img ..."
  docker pull -q "$img" >/dev/null 2>&1 && echo "    ok" || echo "    ❌ 失败"
done

echo "=== 3. 起 PD ==="
docker rm -f pd >/dev/null 2>&1 || true
mkdir -p /data/pd /data/tikv
docker run -d --name pd --restart unless-stopped \
  -p 2379:2379 -p 2380:2380 \
  -v /data/pd:/data \
  pingcap/pd:v7.5.0 \
  --name=pd --data-dir=/data \
  --client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://$PRIV:2379 \
  --peer-urls=http://0.0.0.0:2380 \
  --advertise-peer-urls=http://$PRIV:2380 \
  --initial-cluster=pd=http://$PRIV:2380 >/dev/null
sleep 8
echo "  PD 容器: $(docker ps --filter name=pd --format '{{.Status}}')"

echo "=== 4. 起 TiKV ==="
docker rm -f tikv >/dev/null 2>&1 || true
docker run -d --name tikv --restart unless-stopped \
  -p 20160:20160 -p 20180:20180 \
  -v /data/tikv:/data \
  pingcap/tikv:v7.5.0 \
  --addr=0.0.0.0:20160 \
  --advertise-addr=$PRIV:20160 \
  --status-addr=0.0.0.0:20180 \
  --data-dir=/data \
  --pd-endpoints=$PRIV:2379 >/dev/null
sleep 20
echo "  TiKV 容器: $(docker ps --filter name=tikv --format '{{.Status}}')"

echo "=== 5. 连通性自检 ==="
curl -s --max-time 5 http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null | head -c 400 | sed 's/^/  /'
echo
echo "  PD 端口: $(ss -ltn 2>/dev/null | grep -c 2379)  TiKV 端口: $(ss -ltn 2>/dev/null | grep -c 20160)"
echo
echo "=== 6. 资源 ==="
free -g | sed -n 2p | sed 's/^/  /'
df -h / | tail -1 | sed 's/^/  /'
