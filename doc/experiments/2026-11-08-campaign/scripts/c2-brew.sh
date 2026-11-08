#!/bin/bash
# C2 在 BrewFS 上补跑：pytest-full-02（短）与 django（长）
# 用法：c2-brew.sh pytest | django
set -u
K=/root/.ssh/ax-c1-20261106
TRACE="${1:?pytest|django}"
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' "$TRACE" <<'EOS' 2>&1
set -u
TRACE="$1"
BIN=/srv/ax-data/artifacts/bin/full-fuse-replay
case "$TRACE" in
  pytest) C=/srv/ax-data/traces/extracted/pytest-full-02; MNT=/mnt/bfs-c1fio; NAME=pytest-full-02 ;;
  django) C=/srv/ax-data/traces/extracted/django;          MNT=/mnt/bfs-c1fio; NAME=django ;;
  *) echo "unknown trace"; exit 2 ;;
esac
ROOT=$MNT/c2-$NAME-01
OUT=/srv/ax-data/c2b2/brew-$NAME
mkdir -p /srv/ax-data/c2b2
rm -rf "$ROOT" 2>/dev/null
mkdir -p "$ROOT"
if [ -d "$C/initial" ]; then cp -a "$C/initial/." "$ROOT/" 2>/dev/null; fi
N0=$(find "$ROOT" -type f 2>/dev/null | wc -l)
echo "start $(date -u +%FT%TZ) root=$ROOT files=$N0"
echo "cmd: $BIN $C/capture/events.jsonl $C/capture/blobs $ROOT $OUT.json"
T0=$(date +%s)
timeout 10800 "$BIN" "$C/capture/events.jsonl" "$C/capture/blobs" "$ROOT" "$OUT.json" > "$OUT.out" 2>&1
RC=$?
T1=$(date +%s)
echo "exit=$RC wall=$((T1-T0))s end=$(date -u +%FT%TZ)"
python3 - "$OUT.json" <<'PY'
import json, os, sys
p = sys.argv[1]
if not os.path.exists(p):
    print("  result 缺失"); raise SystemExit
try:
    j = json.load(open(p))
except Exception as e:
    print("  JSON 解析失败:", e); raise SystemExit
def g(*keys):
    for k in keys:
        if k in j: return j[k]
    return None
print("  events:", g("events","total_events","event_count"))
print("  ops:", g("ops","total_ops"))
print("  callbacks:", g("callbacks"))
print("  reads_bytes:", g("read_bytes","reads_bytes"))
print("  writes_bytes:", g("write_bytes","writes_bytes"))
print("  errors:", g("errors","error_count"))
print("  duration_s:", g("duration_s","wall_seconds","elapsed_s"))
PY
EOS
