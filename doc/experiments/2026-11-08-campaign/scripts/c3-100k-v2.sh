#!/bin/bash
# C3 100k pilot ×3（只建一次 workspace，之后用「恢复被改文件」的方式复位）
set -u
B=/srv/ax-data/artifacts/bin/smallfiles
SRC=/mnt/brewfs-c3/base/small
WS=/mnt/brewfs-c3/base/ws100k
OUT=/srv/ax-data/c3-100k
mkdir -p $OUT
TSV=$OUT/pilot.tsv
printf "rep\tbuild_s\twall_s\treads\twrites\tfiles\trestored\n" > "$TSV"

echo "=== 1. 补齐 workspace（幂等 cp，103 目录）==="
mkdir -p "$WS"
T0=$(date +%s)
i=0
for d in $(ls $SRC | head -103); do
  [ -d "$WS/$d" ] || cp -a "$SRC/$d" "$WS/$d" 2>/dev/null
  i=$((i+1))
  [ $((i % 20)) -eq 0 ] && echo "  已处理 $i/103"
done
T1=$(date +%s)
N=$(find "$WS" -type f | wc -l)
echo "  build=$((T1-T0))s files=$N"
[ "$N" -lt 90000 ] && { echo "  文件数不足，退出"; exit 1; }

run_pilot() {
  local rep=$1
  sync; echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
  timeout 1800 $B run --root "$WS" --output "$OUT/r$rep/run" \
    --files 100000 --hot-files 512 --workers 8 \
    --read-ops 8192 --cow-files 256 --write-ops 256 --mixed-ops 1024 \
    --seed 9173 --workspace 1 > "$OUT/r$rep.out" 2>&1
  echo "  pilot rc=$?"
}

echo "=== 2. rep1 ==="
touch $OUT/marker1
run_pilot 1
python3 - "$OUT/r1/run/result.json" "$TSV" 1 "$((T1-T0))" "$N" 0 <<'PY'
import json, os, sys
p, tsv, rep, bs, nf, rs = sys.argv[1:7]
if not os.path.exists(p): print("  rep1 result 缺失"); raise SystemExit
j = json.load(open(p)); w = j.get("wall_seconds", 0)
open(tsv,"a").write(f"{rep}\t{bs}\t{w:.4f}\t{j.get('verified_reads')}\t{j.get('writes')}\t{nf}\t{rs}\n")
print(f"  rep1: wall={w:.4f}s reads={j.get('verified_reads')}")
PY

echo "=== 3. 找出被修改的文件（比 marker 新）==="
find "$WS" -type f -newer $OUT/marker1 > $OUT/touched.txt 2>/dev/null
echo "  touched: $(wc -l < $OUT/touched.txt)"

restore() {
  local n=0
  while IFS= read -r f; do
    rel=${f#$WS/}
    src="$SRC/$rel"
    if [ -f "$src" ]; then
      rm -f "$f" 2>/dev/null
      cp -a "$src" "$f" 2>/dev/null && n=$((n+1))
    fi
  done < $OUT/touched.txt
  echo "  restored=$n"
}

for rep in 2 3; do
  echo "=== rep$rep 复位 + 跑 ==="
  T2=$(date +%s)
  restore
  T3=$(date +%s)
  run_pilot $rep
  python3 - "$OUT/r$rep/run/result.json" "$TSV" "$rep" "$((T3-T2))" "$N" "$((T3-T2))" <<'PY'
import json, os, sys
p, tsv, rep, bs, nf, rs = sys.argv[1:7]
if not os.path.exists(p): print(f"  rep{rep} result 缺失"); raise SystemExit
j = json.load(open(p)); w = j.get("wall_seconds", 0)
open(tsv,"a").write(f"{rep}\t0\t{w:.4f}\t{j.get('verified_reads')}\t{j.get('writes')}\t{nf}\t{rs}\n")
print(f"  rep{rep}: wall={w:.4f}s reads={j.get('verified_reads')}")
PY
done

echo
echo "=== 结果 ==="
cat "$TSV" | sed 's/^/  /'
