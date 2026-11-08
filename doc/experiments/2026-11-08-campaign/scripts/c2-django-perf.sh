#!/bin/bash
# django 在 BrewFS 上（performance 模式）限时 30 分钟采样，用于外推
set -u
SRC=/srv/ax-data/traces/extracted/django
OUTD=/srv/ax-data/c2b2/perf-django
mkdir -p $OUTD
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
B=/srv/ax-data/artifacts/bin/brewfs-patched
mkdir -p /mnt/bfs-c2perf
echo "  mount=$(mountpoint -q /mnt/bfs-c2perf && echo y || echo n)"
if ! mountpoint -q /mnt/bfs-c2perf; then
  setsid nohup $B mount --privileged \
    --meta-backend tikv --meta-tikv-pd-endpoints http://10.20.0.12:2379 \
    --data-backend s3 --s3-bucket c2perf-20261108 --s3-endpoint http://10.20.0.13:9000 \
    --s3-region us-east-1 --s3-force-path-style true \
    /mnt/bfs-c2perf > $OUTD/mount.log 2>&1 &
  for i in $(seq 1 60); do sleep 1; mountpoint -q /mnt/bfs-c2perf && break; done
fi
ROOT=/mnt/bfs-c2perf/django-01
rm -rf "$ROOT"; mkdir -p "$ROOT"
[ -d "$SRC/initial" ] && cp -a "$SRC/initial/." "$ROOT/" 2>/dev/null
echo "  initial files=$(find "$ROOT" -type f | wc -l)"
T0=$(date +%s)
timeout 1800 /srv/ax-data/artifacts/bin/full-fuse-replay \
  "$SRC/capture/events.jsonl" "$SRC/capture/blobs" "$ROOT" "$OUTD/result.json" --performance \
  > "$OUTD/replay.out" 2>&1
RC=$?
T1=$(date +%s)
echo "  replay rc=$RC (timeout=1800s) wall=$((T1-T0))s"
tail -2 "$OUTD/replay.out" 2>/dev/null | cut -c1-160 | sed 's/^/  /'
python3 - "$OUTD/replay.out" <<'PY'
import json, sys, re
lines = [l for l in open(sys.argv[1], errors="replace") if l.strip().startswith("{")]
if not lines:
    print("  无进度行"); raise SystemExit
last = json.loads(lines[-1])
ops, secs = last["operations"], last["seconds"]
print(f"  采样: ops={ops} seconds={secs:.1f} rate={ops/secs:.0f} ops/s")
total = 57787980
print(f"  外推全量: {total/ (ops/secs) / 3600:.2f} 小时")
PY
