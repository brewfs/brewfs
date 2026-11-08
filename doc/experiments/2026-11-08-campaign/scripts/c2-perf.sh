#!/bin/bash
# C2 正解：用 --performance 模式（该模式容忍 xattr 的 errno 差异，会记 probe_errno_differences）
set -u
TRACE="${1:?pytest|django}"
case "$TRACE" in
  pytest) SRC=/srv/ax-data/traces/extracted/pytest-full-02; NAME=pytest-full-02 ;;
  django) SRC=/srv/ax-data/traces/extracted/django;          NAME=django ;;
  *) echo unknown; exit 2 ;;
esac
OUTD=/srv/ax-data/c2b2/perf-$NAME
mkdir -p $OUTD
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
B=/srv/ax-data/artifacts/bin/brewfs-patched
ssh -o StrictHostKeyChecking=no -i /root/.ssh/ax-c1-20261106 root@10.20.0.13 "mkdir -p /srv/ax-data/rustfs/c2perf-20261108" 2>/dev/null
mkdir -p /mnt/bfs-c2perf
pkill -f 'brewfs-patched mount.*bfs-c2perf' 2>/dev/null || true
sleep 2; umount -l /mnt/bfs-c2perf 2>/dev/null || true
setsid nohup $B mount --privileged \
  --meta-backend tikv --meta-tikv-pd-endpoints http://10.20.0.12:2379 \
  --data-backend s3 --s3-bucket c2perf-20261108 --s3-endpoint http://10.20.0.13:9000 \
  --s3-region us-east-1 --s3-force-path-style true \
  /mnt/bfs-c2perf > $OUTD/mount.log 2>&1 &
for i in $(seq 1 60); do sleep 1; mountpoint -q /mnt/bfs-c2perf && break; done
echo "  mountpoint=$(mountpoint -q /mnt/bfs-c2perf && echo y || echo n) after ${i}s"
[ "$(mountpoint -q /mnt/bfs-c2perf && echo y || echo n)" = "n" ] && { tail -5 $OUTD/mount.log | cut -c1-140; exit 1; }

ROOT=/mnt/bfs-c2perf/$NAME-01
rm -rf "$ROOT"; mkdir -p "$ROOT"
[ -d "$SRC/initial" ] && cp -a "$SRC/initial/." "$ROOT/" 2>/dev/null
echo "  initial files=$(find "$ROOT" -type f | wc -l)"
T0=$(date +%s)
timeout 10800 /srv/ax-data/artifacts/bin/full-fuse-replay \
  "$SRC/capture/events.jsonl" "$SRC/capture/blobs" "$ROOT" "$OUTD/result.json" --performance \
  > "$OUTD/replay.out" 2>&1
RC=$?
T1=$(date +%s)
echo "  replay rc=$RC wall=$((T1-T0))s"
tail -3 "$OUTD/replay.out" 2>/dev/null | cut -c1-160 | sed 's/^/  /'
if [ -s "$OUTD/result.json" ]; then
  python3 - "$OUTD/result.json" <<'PY'
import json, sys
j = json.load(open(sys.argv[1]))
for k in ("mode","completed","operations","expected_errors","probe_errno_differences",
          "actual_read_bytes","actual_write_bytes","execution_seconds","active_replay_seconds","remaining_handles"):
    if k in j: print(f"  {k}: {j[k]}")
PY
else
  echo "  result 缺失"
fi
umount /mnt/bfs-c2perf 2>/dev/null; echo "  umount rc=$?"
