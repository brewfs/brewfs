#!/bin/bash
# C1-v2 写类矩阵补测：bs=4M/64M × write/randwrite/randrw × 3 reps × 2 sys + 饱和组
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.161 'bash -s' <<'EOS' 2>&1
set -u
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
OUT=/srv/ax-data/c1v2
summarize() {
  python3 - "$@" <<'PY' >> $OUT/run.log 2>&1
import json,sys,os
p,sysn,op,bs,rep,mode,ms = sys.argv[1:8]
if not os.path.exists(p): print(f"  {sysn}-{op}-{bs}-r{rep} FAILED"); raise SystemExit
j=json.load(open(p))["jobs"][0]; w=j.get("write",{}) or {}; rd=j.get("read",{}) or {}
src = w if w.get("io_bytes",0)>=rd.get("io_bytes",0) else rd
pct=(src.get("clat_ns") or {}).get("percentile") or {}
g=lambda k: (pct.get(k)/1e6 if pct.get(k) else "")
bw=(w.get("bw_bytes",0)+rd.get("bw_bytes",0))/2**20
print(f"{sysn}-{op}-{bs}-r{rep}({mode}): bw={bw:.1f} rdBW={rd.get('bw_bytes',0)/2**20:.1f} p50={g('50.000000')} p95={g('95.000000')} p99={g('99.000000')} wall={int(ms)/1000:.1f}s")
with open("/srv/ax-data/c1v2/c1v2.tsv","a") as f:
    f.write(f"{sysn}\t{op}\t{bs}\t{rep}\t{mode}\t{bw:.2f}\t{rd.get('bw_bytes',0)/2**20:.2f}\t"
            f"{w.get('iops',0)+rd.get('iops',0):.0f}\t{g('50.000000')}\t{g('95.000000')}\t{g('99.000000')}\t"
            f"{int(ms)/1000:.2f}\t{rd.get('io_bytes',0)/2**20:.1f}\t{w.get('io_bytes',0)/2**20:.1f}\t0\n")
PY
}
echo "=== write-matrix 4M/64M start ==="
for bs in 4M 64M; do
  for op in write randwrite randrw; do
    for rep in 1 2 3; do
      for sys in juicefs brewfs; do
        if [ "$sys" = "juicefs" ]; then dir=/mnt/jfs-c1v2; else dir=/mnt/bfs-c1v2; fi
        mountpoint -q $dir || { echo "  $sys not mounted, skip"; continue; }
        if [ $rep -eq 1 ]; then mode=create; else mode=overwrite; fi
        fname="m-$sys-$op-$bs-r1.dat"
        case $op in
          write) RW="--rw=write --end_fsync=1";;
          randwrite) RW="--rw=randwrite --end_fsync=1";;
          randrw) RW="--rw=randrw --rwmixread=70";;
        esac
        T0=$(date +%s%N)
        fio --name="$sys-$op-$bs-r$rep" --directory=$dir --filename=$fname --bs=$bs --size=4G \
            --direct=1 --ioengine=libaio --iodepth=16 --numjobs=1 --percentile_list=50:95:99 \
            $RW --output-format=json --output=$OUT/$sys-$op-$bs-r$rep.json >/dev/null 2>&1
        T1=$(date +%s%N)
        summarize $OUT/$sys-$op-$bs-r$rep.json $sys $op $bs $rep $mode $(( (T1-T0)/1000000 ))
        tail -1 $OUT/run.log | cut -c1-165 | sed 's/^/  /'
      done
    done
  done
  for sys in juicefs brewfs; do
    if [ "$sys" = "juicefs" ]; then dir=/mnt/jfs-c1v2; else dir=/mnt/bfs-c1v2; fi
    mountpoint -q $dir || continue
    T0=$(date +%s%N)
    fio --name="$sys-write_sat-$bs" --directory=$dir --filename="sat-$sys-$bs.dat" --rw=write --bs=$bs --size=4G \
        --direct=1 --ioengine=libaio --iodepth=32 --numjobs=8 --end_fsync=1 --percentile_list=50:95:99 \
        --output-format=json --output=$OUT/$sys-write_sat-$bs.json >/dev/null 2>&1
    T1=$(date +%s%N)
    summarize $OUT/$sys-write_sat-$bs.json $sys write_sat $bs 1 sat $(( (T1-T0)/1000000 ))
    tail -1 $OUT/run.log | cut -c1-165 | sed 's/^/  /'
  done
done
rm -f /mnt/jfs-c1v2/*.dat /mnt/bfs-c1v2/*.dat 2>/dev/null || true
echo "=== write-matrix done ==="
EOS
