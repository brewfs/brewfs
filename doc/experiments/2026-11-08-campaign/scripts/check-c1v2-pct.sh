#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.161 'bash -s' <<'EOS' 2>&1
set -u
F=/srv/ax-data/c1v2/juicefs-write-1M-r1.json
python3 - "$F" <<'PY'
import json,sys
j=json.load(open(sys.argv[1]))["jobs"][0]
w=j.get("write",{})
lat=(w.get("clat_ns") or {}).get("percentile") or {}
print("  clat_ns.percentile:", lat)
print("  write.bw_bytes:", w.get("bw_bytes"), " iops:", w.get("iops"), " io_bytes:", w.get("io_bytes"))
print("  job_runtime_ms:", j.get("job_runtime"))
print("  lat_ns(min/max/mean):", (w.get("lat_ns") or {}).get("min"), (w.get("lat_ns") or {}).get("max"))
PY
EOS
