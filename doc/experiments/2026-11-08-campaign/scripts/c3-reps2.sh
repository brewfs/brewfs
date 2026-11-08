#!/bin/bash
# C3 v2（小规模 1 万文件 × 3 轮，低成本拿方差）
set -u
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
export RUST_LOG=warn
B=/srv/ax-data/artifacts/bin/brewfs-patched
SM=/srv/ax-data/artifacts/bin/smallfiles
OUT=/srv/ax-data/c3v2
mkdir -p $OUT

echo "=== 1. 干净重挂（warn 日志，避免日志爆炸）==="
pkill -f 'brewfs-patched mount.*bfs-c3v' 2>/dev/null || true
sleep 3
umount -l /mnt/bfs-c3v 2>/dev/null || true
mkdir -p /mnt/bfs-c3v
setsid nohup $B mount --privileged \
  --meta-backend tikv --meta-tikv-pd-endpoints http://10.20.0.12:2379 \
  --data-backend s3 --s3-bucket c1fio-20261108-brew --s3-endpoint http://10.20.0.13:9000 \
  --s3-region us-east-1 --s3-force-path-style true \
  /mnt/bfs-c3v > $OUT/mount.log 2>&1 &
for i in $(seq 1 60); do sleep 1; mountpoint -q /mnt/bfs-c3v && break; done
echo "  mount=$(mountpoint -q /mnt/bfs-c3v && echo y || echo n) procs=$(pgrep -fc 'brewfs-patched mount' || echo 0)"
[ "$(mountpoint -q /mnt/bfs-c3v && echo y || echo n)" = "n" ] && exit 1
rm -rf /mnt/bfs-c3v/base 2>/dev/null || true
mkdir -p /mnt/bfs-c3v/base/small

echo "=== 2. 生成 10,000 文件（10 目录 × 1000，逐目录重试）==="
python3 - <<'PY'
import os, time
root = "/mnt/bfs-c3v/base/small"
payload = b"x" * 4096
t0 = time.time(); n = 0
for d in range(10):
    dp = os.path.join(root, f"d{d:04d}")
    for attempt in range(3):
        try:
            os.makedirs(dp, exist_ok=True)
            for i in range(1000):
                with open(os.path.join(dp, f"f{i:05d}"), "wb") as f:
                    f.write(payload)
                n += 1
            break
        except OSError as e:
            print(f"  d{d:04d} attempt {attempt+1} failed: {e}")
            time.sleep(1)
    else:
        print(f"  d{d:04d} 放弃")
el = time.time() - t0
print(f"  DONE files={n} elapsed={el:.1f}s rate={n/max(el,1e-9):.0f} files/s")
PY
echo "  files=$(find /mnt/bfs-c3v/base/small -type f | wc -l)"

echo "=== 3. 三轮 pilot + 恢复 ==="
: > $OUT/pilot.tsv
printf "rep\twall_s\trestore_files\trestore_s\n" >> $OUT/pilot.tsv
for rep in 1 2 3; do
  MARK=$OUT/marker-$rep; touch $MARK
  rm -rf $OUT/p$rep; mkdir -p $OUT/p$rep
  T0=$(date +%s%N)
  timeout 900 $SM run --root /mnt/bfs-c3v/base --output $OUT/p$rep/run \
    --files 10000 --hot-files 256 --workers 8 \
    --read-ops 4096 --cow-files 128 --write-ops 128 --mixed-ops 512 \
    --seed 9173 --workspace 1 > $OUT/p$rep/run.out 2>&1
  RC=$?
  T1=$(date +%s%N)
  WALL=$(python3 -c "print(f'{($T1-$T0)/1e9:.4f}')")
  echo "  rep$rep rc=$RC wall=${WALL}s"
  tail -2 $OUT/p$rep/run.out | cut -c1-120 | sed 's/^/    /'
  T2=$(date +%s%N)
  N=$(find /mnt/bfs-c3v/base/small -type f -newer $MARK 2>/dev/null | wc -l)
  find /mnt/bfs-c3v/base/small -type f -newer $MARK 2>/dev/null | while read -r f; do
    printf 'x%.0s' {1..4096} > "$f" 2>/dev/null || true
  done
  T3=$(date +%s%N)
  RS=$(python3 -c "print(f'{($T3-$T2)/1e9:.2f}')")
  echo "    恢复=$N 文件, ${RS}s"
  printf "%s\t%s\t%s\t%s\n" "$rep" "$WALL" "$N" "$RS" >> $OUT/pilot.tsv
done
echo "=== 汇总 ==="
cat $OUT/pilot.tsv | sed 's/^/  /'
python3 - "$OUT/pilot.tsv" <<'PY'
import csv, statistics, sys
rows = list(csv.DictReader(open(sys.argv[1]), delimiter="\t"))
w = [float(r["wall_s"]) for r in rows if r.get("wall_s")]
if w:
    print(f"  wall: median={statistics.median(w):.4f}s min={min(w):.4f}s max={max(w):.4f}s n={len(w)}")
PY
umount /mnt/bfs-c3v 2>/dev/null; echo "  umount rc=$?"
