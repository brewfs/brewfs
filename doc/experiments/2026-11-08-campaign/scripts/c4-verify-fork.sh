#!/bin/bash
# 验证 fork 出的 workspace：能看到 base 文件 + 写入隔离（base root_hash 不变）
set -u
K=/root/.ssh/ax-c1-20261106
cat > /tmp/pickfork.py <<'PY'
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
        if isinstance(obj, list) and obj and 'workspace_id' in obj[0]:
            best = obj; break
    if best: break
if not best:
    sys.exit("PARSE_FAIL")
base = best[0]
forked = [w for w in best if w['workspace_id'] != base['workspace_id'] and (w.get('owner_id') == 'c4f')]
print("base_id", base['workspace_id'])
print("base_ver", base['base_revision']['sealed_version'])
print("forked_id", (forked[0]['workspace_id'] if forked else ""))
print("fork_count", len(forked))
PY
scp -q -o StrictHostKeyChecking=no -i $K /tmp/pickfork.py root@10.20.0.11:/root/pickfork.py
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
echo "=== 1. 选一个 fork 出来的 workspace ==="
timeout 120 $B workspace list --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS 2>/dev/null | python3 /root/pickfork.py | sed 's/^/  /'
FID=$(timeout 120 $B workspace list --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS 2>/dev/null | python3 /root/pickfork.py | awk '$1=="forked_id"{print $2}')
echo "  FID=$FID"
echo
echo "=== 2. 挂载 fork 出的 workspace ==="
mkdir -p /mnt/c4fork
pkill -f 'brewfs mount.*c4fork' 2>/dev/null; umount -l /mnt/c4fork 2>/dev/null || true
setsid nohup $B mount --privileged --volume-format workspace-v1 \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS \
  --workspace "$FID" \
  --data-backend s3 --s3-bucket c4ws-20261108 --s3-endpoint http://10.20.0.13:9000 --s3-region us-east-1 \
  /mnt/c4fork > /srv/ax-data/c4ws/mount-fork.log 2>&1 &
for i in $(seq 1 40); do sleep 1; mountpoint -q /mnt/c4fork && break; done
echo "  mountpoint: $(mountpoint -q /mnt/c4fork && echo y || echo n)"
echo
echo "=== 3. fork 里能看到 base 的文件吗 ==="
echo "  base 目录数: $(ls /mnt/c4fork/base 2>/dev/null | wc -l)"
echo "  base 文件数: $(find /mnt/c4fork/base -type f 2>/dev/null | wc -l)"
F=$(find /mnt/c4fork/base -type f 2>/dev/null | head -1)
echo "  抽样: $F size=$(stat -c%s "$F" 2>/dev/null)"
echo
echo "=== 4. 隔离性：在 fork 里写新文件 + 改一个文件 ==="
echo "isolation-marker" > /mnt/c4fork/base/FORK_ONLY.txt 2>&1 && echo "  写新文件 OK" || echo "  写新文件 FAIL"
[ -n "$F" ] && echo "overwritten-by-fork" > "$F" 2>&1 && echo "  改已有文件 OK" || echo "  改已有文件 FAIL"
sleep 1
umount /mnt/c4fork 2>/dev/null; echo "  umount rc=$?"
EOS
