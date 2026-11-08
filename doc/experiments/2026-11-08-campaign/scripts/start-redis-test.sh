#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== 1. 起 Redis（jump，10.20.0.10:6379）==="
pkill -x redis-server 2>/dev/null || true
sleep 1
setsid nohup redis-server --bind 0.0.0.0 --port 6379 --save '' --appendonly no \
  --daemonize no --maxmemory 2gb --maxmemory-policy noeviction \
  > /var/log/redis-c4.log 2>&1 &
sleep 3
echo "  ping: $(redis-cli -h 127.0.0.1 ping 2>&1)"
echo
echo "=== 2. 开工 a-* ==="
for id in i-bp1gnsosw4lh1uudhtme i-bp13o0d2f5q95jysu3ez i-bp13i3cmn1iuislukj1n; do
  aliyun ecs StartInstance --RegionId cn-hangzhou --InstanceId $id >/dev/null 2>&1 || true
done
sleep 70
for h in 10.20.0.11 10.20.0.12 10.20.0.13; do
  ping -c1 -W2 $h >/dev/null 2>&1 && echo "  $h up" || echo "  $h down"
done
