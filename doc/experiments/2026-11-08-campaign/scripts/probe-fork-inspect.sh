#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.164 'bash -s' <<'EOS' 2>&1
set -u
cd /srv/ax-data/src || exit 1
B=target/release/brewfs
META="sqlite:///srv/ax-data/sealtest/meta.db"
FID=$(python3 - <<'PY'
import json,re
raw=open('/srv/ax-data/sealtest/fork.json',errors='replace').read()
m=re.findall(r'"workspace_id":\s*"([0-9a-f-]{36})"', raw)
print(m[0] if m else "")
PY
)
echo "  FID=$FID"
echo "=== workspace inspect ==="
$B workspace inspect --meta-backend sqlx --meta-url "$META" "$FID" 2>&1 | grep -v 'INFO' | head -12 | cut -c1-140 | sed 's/^/  /'
echo "=== workspace diff（fork vs base）==="
$B workspace diff --meta-backend sqlx --meta-url "$META" "$FID" 2>&1 | grep -v 'INFO' | head -8 | cut -c1-140 | sed 's/^/  /'
echo "=== 挂载日志里的 lower/base 线索 ==="
grep -iE 'lower|base|layer|attach' /srv/ax-data/sealtest/mount-fork.log 2>/dev/null | head -6 | cut -c1-140 | sed 's/^/  /'
EOS
