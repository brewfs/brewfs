#!/bin/bash
# 测「fork → 可用」端到端延迟：fork 已经是 62ms，再测挂载耗时
set -u
echo "=== 1. 开机 ==="
for id in i-bp1cp8ccqor34u2bo5os i-bp13o0d2f5q95jysu3ez i-bp13i3cmn1iuislukj1n i-bp1gnsosw4lh1uudhtme; do
  aliyun ecs StartInstance --RegionId cn-hangzhou --InstanceId $id >/dev/null 2>&1 || true
done
sleep 75
for h in 10.20.0.12 10.20.0.13 10.20.0.11; do
  ping -c1 -W2 $h >/dev/null 2>&1 && echo "  $h up" || echo "  $h 未就绪"
done
