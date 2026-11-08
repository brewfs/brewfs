#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
python3 - <<'PY'
import json, os
p = '/srv/ax-data/c1fio/juicefs-randrw-1M-r1.json'
j = json.load(open(p))['jobs'][0]
print(" 顶层 latency_ns keys:", list((j.get('latency_ns') or {}).keys()))
lat = j.get('latency_ns') or {}
bins = lat.get('bins') or {}
print(" bins 前 3:", list(bins.items())[:3])
print(" bins 数量:", len(bins))
# 试算 p95：bins 的 key 形如 "1000" 或 ">=..."，value 是次数
PY
EOS
