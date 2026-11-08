#!/bin/bash
# revision 格式是 layer_id:sealed_version:root_hash(hex?)，取 inspect 的完整字段拼出来
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
WID=01a1b6e7-db96-7fb2-ae2a-69cf7851bec6
echo "=== inspect JSON → 提取 base_revision 三元组 ==="
timeout 90 $B workspace inspect --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS $WID 2>/dev/null | python3 - <<'PY'
import json, sys, re
raw = sys.stdin.read()
m = re.search(r'\{[\s\S]*\}', raw)
d = json.loads(m.group(0))
rev = d.get("base_revision", {})
layer = rev.get("layer_id"); ver = rev.get("sealed_version"); rh = rev.get("root_hash")
hexs = ''.join(f'{b:02x}' for b in rh) if isinstance(rh, list) else str(rh)
print("  layer:", layer)
print("  version:", ver)
print("  root_hash_hex:", hexs[:32], "len", len(hexs))
open("/tmp/c4-rev.txt","w").write(f"{layer}:{ver}:{hexs}")
print("  revision string written to /tmp/c4-rev.txt")
PY
REV=$(cat /tmp/c4-rev.txt 2>/dev/null)
echo "  REV=$REV"
echo
echo "=== fork count=1 ==="
T0=$(date +%s%N)
timeout 180 $B workspace fork "$REV" --count 1 --data-config /srv/ax-data/c4ws/data.yaml \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS --owner c4f 2>&1 | tail -5 | cut -c1-150 | sed 's/^/  /'
T1=$(date +%s%N)
echo "  耗时: $(( (T1-T0)/1000000 )) ms"
EOS
