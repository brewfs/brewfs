#!/bin/bash
# 修正：fork 出的 workspace 用 fork_base（不是 base_revision）；挂载后验证
set -u
K=/root/.ssh/ax-c1-20261106
cat > /tmp/pickfork2.py <<'PY'
import json, re, sys
raw = re.sub(r'\x1b\[[0-9;]*[A-Za-z]', '', sys.stdin.read())
best = None
for s in [m.start() for m in re.finditer(r'\[', raw)]:
    for e in range(len(raw), s, -1):
        if raw[e-1] != ']':
            continue
        try:
            obj = json.loads(raw[s:e])
        except Exception:
            continue
        if isinstance(obj, list) and obj and isinstance(obj[0], dict) and 'workspace_id' in obj[0]:
            best = obj; break
    if best: break
if not best:
    sys.exit("PARSE_FAIL")
base = best[0]
others = [w for w in best if w['workspace_id'] != base['workspace_id']]
print("total", len(best))
print("base_id", base['workspace_id'])
if others:
    print("forked_id", others[0]['workspace_id'])
    print("forked_owner", others[0].get('owner_id'))
    print("forked_keys", ",".join(sorted(others[0].keys()))[:200])
PY
scp -q -o StrictHostKeyChecking=no -i $K /tmp/pickfork2.py root@10.20.0.11:/root/pickfork2.py
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
LIST=$(timeout 120 $B workspace list --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS 2>/dev/null)
echo "=== 解析 ==="
echo "$LIST" | python3 /root/pickfork2.py | sed 's/^/  /'
FID=$(echo "$LIST" | python3 /root/pickfork2.py | awk '$1=="forked_id"{print $2}')
echo "  FID=$FID"
[ -z "$FID" ] && { echo "  无 fork 的 workspace"; exit 1; }
echo
echo "=== 挂载 fork ==="
mkdir -p /mnt/c4fork
pkill -f 'brewfs mount.*c4fork' 2>/dev/null; umount -l /mnt/c4fork 2>/dev/null || true
setsid nohup $B mount --privileged --volume-format workspace-v1 \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS \
  --workspace "$FID" \
  --data-backend s3 --s3-bucket c4ws-20261108 --s3-endpoint http://10.20.0.13:9000 --s3-region us-east-1 \
  /mnt/c4fork > /srv/ax-data/c4ws/mount-fork.log 2>&1 &
for i in $(seq 1 40); do sleep 1; mountpoint -q /mnt/c4fork && break; done
echo "  mountpoint: $(mountpoint -q /mnt/c4fork && echo y || echo n)"
tail -3 /srv/ax-data/c4ws/mount-fork.log 2>/dev/null | cut -c1-130 | sed 's/^/  /'
echo
echo "=== fork 内文件 ==="
echo "  目录数: $(ls /mnt/c4fork 2>/dev/null | wc -l)"
echo "  文件数: $(find /mnt/c4fork -type f 2>/dev/null | wc -l)"
find /mnt/c4fork -type f 2>/dev/null | head -3 | sed 's/^/  /'
EOS
