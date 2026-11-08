#!/bin/bash
# C3 补充：100k 规模 pilot ×3（每次用 cp 重建 workspace，测方差）
set -u
B=/srv/ax-data/artifacts/bin/smallfiles
SRC=/mnt/brewfs-c3/base/small
WS=/mnt/brewfs-c3/base/ws100k
OUT=/srv/ax-data/c3-100k
mkdir -p $OUT
TSV=$OUT/pilot.tsv
[ -f "$TSV" ] || printf "rep\tbuild_s\twall_s\treads\twrites\tfiles\n" > "$TSV"
echo "=== 前置检查 ==="
[ -d "$SRC" ] && echo "  src dirs: $(ls $SRC | wc -l)" || { echo "  src 缺失"; exit 1; }

for rep in 1 2 3; do
  echo "--- rep$rep 重建 workspace ---"
  rm -rf "$WS" 2>/dev/null
  mkdir -p "$WS"
  T0=$(date +%s)
  for d in $(ls $SRC | head -103); do
    cp -a "$SRC/$d" "$WS/$d" 2>/dev/null
  done
  T1=$(date +%s)
  N=$(find "$WS" -type f | wc -l)
  echo "  build=$((T1-T0))s files=$N"
  [ "$N" -lt 90000 ] && { echo "  文件数不足，跳过"; continue; }
  sync; echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
  echo "  pilot..."
  timeout 1800 $B run --root "$WS" --output "$OUT/r$rep/run" \
    --files 100000 --hot-files 512 --workers 8 \
    --read-ops 8192 --cow-files 256 --write-ops 256 --mixed-ops 1024 \
    --seed 9173 --workspace 1 > "$OUT/r$rep.out" 2>&1
  RC=$?
  python3 - "$OUT/r$rep/run/result.json" "$TSV" "$rep" "$((T1-T0))" "$N" <<'PY'
import json, os, sys
p, tsv, rep, bs, nf = sys.argv[1:6]
if not os.path.exists(p):
    print(f"  rep{rep}: result.json 缺失 (rc 见 .out)"); raise SystemExit
j = json.load(open(p))
w = j.get("wall_seconds", 0)
open(tsv, "a").write(f"{rep}\t{bs}\t{w:.4f}\t{j.get('verified_reads')}\t{j.get('writes')}\t{nf}\n")
print(f"  rep{rep}: build={bs}s wall={w:.4f}s reads={j.get('verified_reads')} writes={j.get('writes')}")
PY
  echo "  rc=$RC"
done
echo
echo "=== 结果 ==="
cat "$TSV" | sed 's/^/  /'
