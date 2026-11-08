#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
python3 - <<'PY'
import json
j = json.load(open('/srv/ax-data/c1fio/juicefs-randrw-1M-r1.json'))['jobs'][0]
lat = j.get('latency_ns') or {}
print(" latency_ns 全量:", lat)
print(" write.latency_ns:", (j.get('write') or {}).get('latency_ns'))
PY
EOS
