#!/bin/bash
# 稳健解析 workspace list 的 JSON（去 ANSI、取最大 JSON 块），拼 revision 并 fork
set -u
K=/root/.ssh/ax-c1-20261106
cat > /tmp/mkrev.py <<'PY'
import json, re, sys
raw = sys.stdin.read()
raw = re.sub(r'\x1b\[[0-9;]*[A-Za-z]', '', raw)
starts = [m.start() for m in re.finditer(r'[\[{]', raw)]
best = None
for s in starts:
    for e in range(len(raw), s, -1):
        if raw[e-1] not in '}]':
            continue
        try:
            obj = json.loads(raw[s:e])
        except Exception:
            continue
        if isinstance(obj, list) and obj and 'workspace_id' in obj[0]:
            best = obj; break
        if isinstance(obj, dict) and 'workspace_id' in obj:
            best = [obj]; break
    if best:
        break
if not best:
    print("PARSE_FAIL", file=sys.stderr); sys.exit(1)
ws = best[0]
rev = ws.get('base_revision') or ws.get('fork_base')
rh = rev['root_hash']
hexs = ''.join(f'{b:02x}' for b in rh) if isinstance(rh, list) else str(rh)
revstr = f"{rev['layer_id']}:{rev['sealed_version']}:{hexs}"
open('/tmp/c4-rev.txt','w').write(revstr)
print(f"  workspace: {ws['workspace_id']}")
print(f"  revision : {revstr[:60]}…")
PY
scp -q -o StrictHostKeyChecking=no -i $K /tmp/mkrev.py root@10.20.0.11:/root/mkrev.py
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
timeout 90 $B workspace list --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS 2>/dev/null | python3 /root/mkrev.py
REV=$(cat /tmp/c4-rev.txt 2>/dev/null)
echo "  REV_len=${#REV}"
echo
echo "=== fork count=1 ==="
T0=$(date +%s%N)
timeout 180 $B workspace fork "$REV" --count 1 --data-config /srv/ax-data/c4ws/data.yaml \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS --owner c4f 2>&1 | grep -avE 'RLIMIT|src/cli' | tail -6 | cut -c1-160 | sed 's/^/  /'
T1=$(date +%s%N)
echo "  耗时: $(( (T1-T0)/1000000 )) ms"
EOS
