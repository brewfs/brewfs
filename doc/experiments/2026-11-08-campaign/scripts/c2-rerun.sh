#!/bin/bash
# 用带 xattr 的新二进制挂 flat 卷，重跑 C2 pytest-full-02
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
echo "=== 0. 二进制 ==="
ls -lh $B | awk '{print "  "$5, $6, $7, $8}'
echo "  BUILD_RC: $(grep -a BUILD_RC /root/build-xattr.log 2>/dev/null | tail -1)"

echo "=== 1. 挂 flat 卷（TiKV 元数据 + xattr）==="
mkdir -p /mnt/bfs-xattr
pkill -f 'brewfs mount.*bfs-xattr' 2>/dev/null || true
sleep 2; umount -l /mnt/bfs-xattr 2>/dev/null || true
setsid nohup $B mount --privileged \
  --meta-backend tikv --meta-tikv-pd-endpoints http://10.20.0.12:2379 \
  --data-backend s3 --s3-bucket c1fio-20261108-brew --s3-endpoint http://10.20.0.13:9000 \
  --s3-region us-east-1 --s3-force-path-style true \
  /mnt/bfs-xattr > /srv/ax-data/c2b2/mount-xattr.log 2>&1 &
for i in $(seq 1 90); do sleep 1; mountpoint -q /mnt/bfs-xattr && break; done
echo "  mountpoint=$(mountpoint -q /mnt/bfs-xattr && echo y || echo n) after ${i}s"
tail -3 /srv/ax-data/c2b2/mount-xattr.log 2>/dev/null | cut -c1-140 | sed 's/^/  /'
[ "$(mountpoint -q /mnt/bfs-xattr && echo y || echo n)" = "n" ] && exit 1

echo "=== 2. getxattr 语义自测 ==="
touch /mnt/bfs-xattr/xa.txt
python3 - <<'PY'
import os
p = "/mnt/bfs-xattr/xa.txt"
try:
    os.getxattr(p, "security.capability")
    print("  getxattr: 返回值（意外）")
except OSError as e:
    print(f"  getxattr errno={e.errno} ({'ENODATA 正确' if e.errno==61 else 'NOT ENODATA'})")
os.setxattr(p, "user.test", b"v1")
print("  setxattr ok, getxattr =", os.getxattr(p, "user.test"))
print("  listxattr =", os.listxattr(p))
os.removexattr(p, "user.test")
print("  removexattr ok")
PY

echo "=== 3. 重跑 pytest-full-02 ==="
C=/srv/ax-data/traces/extracted/pytest-full-02
ROOT=/mnt/bfs-xattr/c2-pytest-01
OUT=/srv/ax-data/c2b2/xattr-pytest
rm -rf "$ROOT"; mkdir -p "$ROOT"
[ -d "$C/initial" ] && cp -a "$C/initial/." "$ROOT/" 2>/dev/null
N0=$(find "$ROOT" -type f | wc -l)
echo "  initial files=$N0"
T0=$(date +%s)
timeout 1800 /srv/ax-data/artifacts/bin/full-fuse-replay \
  "$C/capture/events.jsonl" "$C/capture/blobs" "$ROOT" "$OUT.json" > "$OUT.out" 2>&1
RC=$?
T1=$(date +%s)
echo "  replay rc=$RC wall=$((T1-T0))s"
tail -4 "$OUT.out" 2>/dev/null | cut -c1-150 | sed 's/^/  /'
python3 - "$OUT.json" <<'PY'
import json, os, sys
p = sys.argv[1]
if not os.path.exists(p): print("  result 缺失"); raise SystemExit
j = json.load(open(p))
print("  ops:", j.get("operations"), " read_bytes:", j.get("read_bytes"), " write_bytes:", j.get("write_bytes"), " seconds:", j.get("seconds"))
PY
EOS
