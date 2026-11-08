#!/bin/bash
# 用传过来的 xattr 二进制挂 flat 卷，重跑 C2 pytest-full-02
set -u
B=/srv/ax-data/artifacts/bin/brewfs-xattr
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
echo "=== 0. 二进制 ==="
ls -lh $B | awk '{print "  "$5, $6, $7, $8}'
chmod +x $B 2>/dev/null || true
echo "  含新 xattr 代码: $(strings -a $B 2>/dev/null | grep -c 'invalid setxattr flags')"

echo "=== 1. 挂 flat 卷（新 bucket，避免旧卷元数据缓存能力位）==="
ssh -o StrictHostKeyChecking=no -i /root/.ssh/ax-c1-20261106 root@10.20.0.13 'mkdir -p /srv/ax-data/rustfs/c2xattr-20261108' 2>/dev/null
mkdir -p /mnt/bfs-c2
pkill -f 'brewfs-xattr mount' 2>/dev/null || true
sleep 2; umount -l /mnt/bfs-c2 2>/dev/null || true
setsid nohup $B mount --privileged \
  --meta-backend tikv --meta-tikv-pd-endpoints http://10.20.0.12:2379 \
  --data-backend s3 --s3-bucket c2xattr-20261108 --s3-endpoint http://10.20.0.13:9000 \
  --s3-region us-east-1 --s3-force-path-style true \
  /mnt/bfs-c2 > /srv/ax-data/c2b2/mount-c2.log 2>&1 &
for i in $(seq 1 60); do sleep 1; mountpoint -q /mnt/bfs-c2 && break; done
echo "  mountpoint=$(mountpoint -q /mnt/bfs-c2 && echo y || echo n) after ${i}s"
if [ "$(mountpoint -q /mnt/bfs-c2 && echo y || echo n)" = "n" ]; then
  tail -6 /srv/ax-data/c2b2/mount-c2.log | cut -c1-150 | sed 's/^/  /'; exit 1
fi

echo "=== 2. getxattr 语义自测 ==="
touch /mnt/bfs-c2/xa.txt
python3 - <<'PY'
import os
p = "/mnt/bfs-c2/xa.txt"
try:
    os.getxattr(p, "security.capability")
    print("  getxattr: 返回（意外）")
except OSError as e:
    print(f"  getxattr errno={e.errno} ({'ENODATA 正确' if e.errno==61 else 'NOT ENODATA'})")
try:
    os.setxattr(p, "user.test", b"v1")
    print("  setxattr ok; getxattr =", os.getxattr(p, "user.test"), "; list =", os.listxattr(p))
    os.removexattr(p, "user.test")
    print("  removexattr ok")
except OSError as e:
    print("  setxattr errno=", e.errno, e.strerror)
PY

echo "=== 3. C2 pytest-full-02 ==="
C=/srv/ax-data/traces/extracted/pytest-full-02
ROOT=/mnt/bfs-c2/c2-pytest-01
OUT=/srv/ax-data/c2b2/xattr-pytest
rm -rf "$ROOT"; mkdir -p "$ROOT"
[ -d "$C/initial" ] && cp -a "$C/initial/." "$ROOT/" 2>/dev/null
echo "  initial files=$(find "$ROOT" -type f | wc -l)"
T0=$(date +%s)
timeout 1800 /srv/ax-data/artifacts/bin/full-fuse-replay \
  "$C/capture/events.jsonl" "$C/capture/blobs" "$ROOT" "$OUT.json" > "$OUT.out" 2>&1
RC=$?
T1=$(date +%s)
echo "  replay rc=$RC wall=$((T1-T0))s"
tail -3 "$OUT.out" 2>/dev/null | cut -c1-150 | sed 's/^/  /'
python3 - "$OUT.json" <<'PY'
import json, os, sys
p = sys.argv[1]
if not os.path.exists(p): print("  result 缺失"); raise SystemExit
j = json.load(open(p))
print("  operations:", j.get("operations"), " read_bytes:", j.get("read_bytes"),
      " write_bytes:", j.get("write_bytes"), " seconds:", j.get("seconds"))
PY
