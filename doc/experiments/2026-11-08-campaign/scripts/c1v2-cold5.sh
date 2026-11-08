#!/bin/bash
# 冷读（bash 版，remount 已验证可用）：3 bs × 2 reps × 2 sys
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.161 'bash -s' <<'EOS' 2>&1
set -u
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
J=/srv/ax-data/artifacts/bin/juicefs
B=/srv/ax-data/artifacts/bin/brewfs-patched
OUT=/srv/ax-data/c1v2
TSV=$OUT/c1v2.tsv
mkdir -p $OUT

remount_j() {
  pkill -f 'juicefs mount' 2>/dev/null; umount -l /mnt/jfs-c1v2 2>/dev/null; sleep 1
  $J mount -d --cache-size 102400 --cache-dir /srv/ax-data/jcache \
    tikv://10.20.2.162:2379/c1fio2 /mnt/jfs-c1v2 >/srv/ax-data/logs/juice-mount.log 2>&1
  for i in $(seq 1 30); do sleep 1; mountpoint -q /mnt/jfs-c1v2 && return 0; done; return 1
}
remount_b() {
  pkill -f 'brewfs-patched mount' 2>/dev/null; umount -l /mnt/bfs-c1v2 2>/dev/null; sleep 1
  setsid nohup $B mount --privileged --config /etc/ax-c1/brewfs-c1v2.yaml /mnt/bfs-c1v2 \
    >/srv/ax-data/logs/brew-mount.log 2>&1 &
  for i in $(seq 1 30); do sleep 1; mountpoint -q /mnt/bfs-c1v2 && return 0; done; return 1
}
restart_rustfs() {
  ssh -o StrictHostKeyChecking=no -i /root/.ssh/ax-c1-20261106 root@10.20.2.163 \
    'pkill -x rustfs; sleep 2; export RUSTFS_ACCESS_KEY=$(cat /etc/ax-c1/rustfs-access); export RUSTFS_SECRET_KEY=$(cat /etc/ax-c1/rustfs-secret); setsid nohup /srv/ax-data/bin/rustfs --address 0.0.0.0:9000 --console-address 0.0.0.0:9001 /srv/ax-data/rustfs </dev/null >>/srv/ax-data/logs/rustfs.log 2>&1 & sleep 5; curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:9000/health' > /tmp/health.txt 2>&1
  echo "$(cat /tmp/health.txt | tail -c 3)"
}
run_cell() { # $1=sys $2=dir $3=bs $4=rep $5=fname $6=mode
  local name="$1-coldread-$3-r$4"
  fio --name="$name" --directory="$2" --filename="$5" --rw=$6 --bs=$3 --size=4G \
      --direct=1 --ioengine=libaio --iodepth=16 --numjobs=1 --percentile_list=50:95:99 \
      --output-format=json --output="$OUT/$name.json" ${7:-} >/dev/null 2>&1
  echo $?
}

echo "=== cold v5 start ==="
for bs in 1M 4M 64M; do
  for rep in 1 2; do
    for sys in juicefs brewfs; do
      if [ "$sys" = "juicefs" ]; then dir=/mnt/jfs-c1v2; remount_j || { echo "  $sys remount fail"; continue; }
      else dir=/mnt/bfs-c1v2; remount_b || { echo "  $sys remount fail"; continue; }; fi
      fname="cold-$sys-$bs.dat"
      # prep 写 4GiB
      fio --name="prep-$sys-$bs-r$rep" --directory=$dir --filename=$fname --rw=write --bs=$bs --size=4G \
          --direct=1 --ioengine=libaio --iodepth=16 --numjobs=1 --end_fsync=1 \
          --output-format=json --output=$OUT/prep5-$sys-$bs-r$rep.json >/dev/null 2>&1 || { echo "  $sys prep fail"; continue; }
      H=$(restart_rustfs)
      sync; echo 3 > /proc/sys/vm/drop_caches
      if [ "$sys" = "juicefs" ]; then remount_j || { echo "  $sys remount2 fail"; continue; }
      else remount_b || { echo "  $sys remount2 fail"; continue; }; fi
      T0=$(date +%s%N)
      fio --name="$sys-coldread-$bs-r$rep" --directory=$dir --filename=$fname --rw=read --bs=$bs --size=4G \
          --direct=1 --ioengine=libaio --iodepth=16 --numjobs=1 --percentile_list=50:95:99 \
          --output-format=json --output=$OUT/$sys-coldread-$bs-r$rep.json >/dev/null 2>&1
      RC=$?; T1=$(date +%s%N)
      python3 - "$OUT/$sys-coldread-$bs-r$rep.json" "$sys" "$bs" "$rep" "$(( (T1-T0)/1000000 ))" "$H" <<'PY' >> $OUT/run.log 2>&1
import json,sys,os
p,sysn,bs,rep,ms,health = sys.argv[1:7]
if not os.path.exists(p): print(f"  {sysn} {bs} r{rep} FAILED no-json"); raise SystemExit
j=json.load(open(p))["jobs"][0]; w=j.get("write",{}) or {}; rd=j.get("read",{}) or {}
src = w if w.get("io_bytes",0)>=rd.get("io_bytes",0) else rd
pct=(src.get("clat_ns") or {}).get("percentile") or {}
p50=pct.get("50.000000"); p95=pct.get("95.000000"); p99=pct.get("99.000000")
bw=rd.get("bw_bytes",0)/2**20
print(f"{sysn}-coldread-{bs}-r{rep}: bw={bw:.1f}MiB/s p50={p50/1e6 if p50 else None}ms p95={p95/1e6 if p95 else None}ms p99={p99/1e6 if p99 else None}ms wall={int(ms)/1000:.1f}s rustfs={health}")
with open("/srv/ax-data/c1v2/c1v2.tsv","a") as f:
    f.write(f"{sysn}\tcoldread\t{bs}\t{rep}\tcold\t{bw:.2f}\t{bw:.2f}\t{rd.get('iops',0):.0f}\t"
            f"{p50/1e6 if p50 else ''}\t{p95/1e6 if p95 else ''}\t{p99/1e6 if p99 else ''}\t"
            f"{int(ms)/1000:.2f}\t{rd.get('io_bytes',0)/2**20:.1f}\t0\t0\n")
PY
      tail -1 $OUT/run.log | cut -c1-160 | sed 's/^/  /'
      rm -f $dir/$fname 2>/dev/null || true
    done
  done
done
echo "=== cold v5 done ==="
EOS
