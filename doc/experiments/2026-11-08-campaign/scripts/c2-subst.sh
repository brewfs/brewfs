#!/bin/bash
# C2 v3：把 xattr 事件替换成等价的 getattr（保持 seq 连续），再回放
set -u
TRACE="${1:?pytest|django}"
case "$TRACE" in
  pytest) SRC=/srv/ax-data/traces/extracted/pytest-full-02; NAME=pytest-full-02 ;;
  django) SRC=/srv/ax-data/traces/extracted/django;          NAME=django ;;
  *) echo "unknown"; exit 2 ;;
esac
OUTD=/srv/ax-data/c2b2/subst-$NAME
mkdir -p $OUTD

echo "=== 1. 替换 xattr → getattr（保 seq）==="
python3 - "$SRC/capture/events.jsonl" "$OUTD/events.jsonl" <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
X = {"getxattr", "setxattr", "listxattr", "removexattr"}
n = subst = 0
with open(src, encoding="utf-8", errors="replace") as f, open(dst, "w", encoding="utf-8") as o:
    for line in f:
        n += 1
        try:
            ev = json.loads(line)
        except Exception:
            o.write(line); continue
        if ev.get("op") in X:
            args = ev.get("args") or []
            path = args[0] if args else "/"
            ev["op"] = "getattr"
            ev["args"] = [path]
            ev["result"] = None
            ev["errno"] = 0
            subst += 1
        o.write(json.dumps(ev, separators=(",", ":")) + "\n")
print(f"  total={n} substituted={subst} ({subst/max(n,1)*100:.2f}%)")
PY

echo "=== 2. 挂 flat BrewFS ==="
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
B=/srv/ax-data/artifacts/bin/brewfs-patched
ssh -o StrictHostKeyChecking=no -i /root/.ssh/ax-c1-20261106 root@10.20.0.13 "mkdir -p /srv/ax-data/rustfs/c2subst-20261108" 2>/dev/null
mkdir -p /mnt/bfs-c2sub
pkill -f 'brewfs-patched mount.*bfs-c2sub' 2>/dev/null || true
sleep 2; umount -l /mnt/bfs-c2sub 2>/dev/null || true
setsid nohup $B mount --privileged \
  --meta-backend tikv --meta-tikv-pd-endpoints http://10.20.0.12:2379 \
  --data-backend s3 --s3-bucket c2subst-20261108 --s3-endpoint http://10.20.0.13:9000 \
  --s3-region us-east-1 --s3-force-path-style true \
  /mnt/bfs-c2sub > $OUTD/mount.log 2>&1 &
for i in $(seq 1 60); do sleep 1; mountpoint -q /mnt/bfs-c2sub && break; done
echo "  mountpoint=$(mountpoint -q /mnt/bfs-c2sub && echo y || echo n) after ${i}s"
[ "$(mountpoint -q /mnt/bfs-c2sub && echo y || echo n)" = "n" ] && { tail -5 $OUTD/mount.log | cut -c1-140; exit 1; }

echo "=== 3. 回放 ==="
ROOT=/mnt/bfs-c2sub/$NAME-01
rm -rf "$ROOT"; mkdir -p "$ROOT"
[ -d "$SRC/initial" ] && cp -a "$SRC/initial/." "$ROOT/" 2>/dev/null
echo "  initial files=$(find "$ROOT" -type f | wc -l)"
T0=$(date +%s)
timeout 10800 /srv/ax-data/artifacts/bin/full-fuse-replay \
  "$OUTD/events.jsonl" "$SRC/capture/blobs" "$ROOT" "$OUTD/result.json" > "$OUTD/replay.out" 2>&1
RC=$?
T1=$(date +%s)
echo "  replay rc=$RC wall=$((T1-T0))s"
tail -3 "$OUTD/replay.out" 2>/dev/null | cut -c1-150 | sed 's/^/  /'
if [ -s "$OUTD/result.json" ]; then
  python3 - "$OUTD/result.json" <<'PY'
import json, sys
j = json.load(open(sys.argv[1]))
print("  operations:", j.get("operations"), " read_bytes:", j.get("read_bytes"),
      " write_bytes:", j.get("write_bytes"), " seconds:", j.get("seconds"))
PY
else
  echo "  result 缺失"
fi
echo "=== 4. 卸载 ==="
umount /mnt/bfs-c2sub 2>/dev/null; echo "  umount rc=$?"
