#!/bin/bash
# C4 闭环：seal → fork 可见性 → 写隔离（需要含 seal 的新二进制）
set -u
B=${1:-/srv/ax-data/artifacts/bin/brewfs-xattr}
PD=http://10.20.0.12:2379
NS=c4ws
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
OUT=/srv/ax-data/c4seal
mkdir -p $OUT
echo "=== 0. 二进制检查 ==="
ls -lh "$B" | awk '{print "  "$5, $6, $7, $8}'
"$B" workspace seal --help 2>&1 | head -6 | sed 's/^/  /'

echo "=== 1. 找一个已有 workspace（c4ws: 01a1b6e7-db96-7fb2-ae2a-69cf7851bec6）==="
WID=01a1b6e7-db96-7fb2-ae2a-69cf7851bec6
echo "  WID=$WID"

echo "=== 2. 挂载它，写入约 1,000 个文件 ==="
mkdir -p /mnt/c4seal
pkill -f 'mount.*c4seal' 2>/dev/null || true
sleep 2; umount -l /mnt/c4seal 2>/dev/null || true
setsid nohup "$B" mount --privileged --volume-format workspace-v1 \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS \
  --workspace "$WID" --data-backend s3 --s3-bucket c4ws-20261108 \
  --s3-endpoint http://10.20.0.13:9000 --s3-region us-east-1 \
  /mnt/c4seal > $OUT/mount.log 2>&1 &
for i in $(seq 1 60); do sleep 1; mountpoint -q /mnt/c4seal && break; done
echo "  mount=$(mountpoint -q /mnt/c4seal && echo y || echo n) after ${i}s"
[ "$(mountpoint -q /mnt/c4seal && echo y || echo n)" = "n" ] && { tail -5 $OUT/mount.log | cut -c1-140; exit 1; }
python3 - <<'PY'
import os, time
dst = "/mnt/c4seal/sealbase"
t0 = time.time()
os.makedirs(dst, exist_ok=True)
payload = b"s" * 4096
n = 0
for d in range(10):
    dp = os.path.join(dst, f"d{d:03d}")
    os.makedirs(dp, exist_ok=True)
    for i in range(100):
        with open(os.path.join(dp, f"f{i:05d}"), "wb") as f:
            f.write(payload)
        n += 1
print(f"  wrote {n} files in {time.time()-t0:.1f}s")
PY
echo "  files=$(find /mnt/c4seal/sealbase -type f 2>/dev/null | wc -l)"
echo "=== 3. 卸载（落盘）==="
T0=$(date +%s%N); umount /mnt/c4seal 2>/dev/null; RC=$?; T1=$(date +%s%N)
echo "  umount rc=$RC wall=$(( (T1-T0)/1000000 ))ms"

echo "=== 4. seal ==="
T0=$(date +%s%N)
SEAL_OUT=$(timeout 600 "$B" workspace seal --meta-backend tikv --meta-tikv-pd-endpoints $PD \
  --workspace-namespace $NS "$WID" 2>&1)
RC=$?
T1=$(date +%s%N)
echo "  seal rc=$RC wall=$(( (T1-T0)/1000000 ))ms"
echo "$SEAL_OUT" | tail -12 | cut -c1-160 | sed 's/^/  /'
REV=$(echo "$SEAL_OUT" | grep -aoE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:[0-9]+:[0-9a-f]+' | head -1)
echo "  REV=${REV:0:60}"
[ -z "$REV" ] && { echo "  ⚠️ 没解析到 revision，手工检查输出"; exit 2; }

echo "=== 5. 记录 seal 前 base 的 root_hash ==="
timeout 90 "$B" workspace list --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS 2>/dev/null \
  | python3 /root/mkrev.py > $OUT/list-before.txt 2>&1 || true
head -3 $OUT/list-before.txt | sed 's/^/  /'

echo "=== 6. fork 3 个 ==="
T0=$(date +%s%N)
timeout 300 "$B" workspace fork "$REV" --count 3 --data-config /srv/ax-data/c4ws/data.yaml \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS --owner c4seal \
  > $OUT/fork.json 2>&1
RC=$?
T1=$(date +%s%N)
echo "  fork rc=$RC wall=$(( (T1-T0)/1000000 ))ms"
NEWID=$(grep -aoE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' $OUT/fork.json | head -1)
echo "  new workspace=$NEWID"

echo "=== 7. 挂 fork，检查能看到 seal 的 1,000 个文件 ==="
mkdir -p /mnt/c4fork2
pkill -f 'mount.*c4fork2' 2>/dev/null || true
sleep 2; umount -l /mnt/c4fork2 2>/dev/null || true
setsid nohup "$B" mount --privileged --volume-format workspace-v1 \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS \
  --workspace "$NEWID" --data-backend s3 --s3-bucket c4ws-20261108 \
  --s3-endpoint http://10.20.0.13:9000 --s3-region us-east-1 \
  /mnt/c4fork2 > $OUT/mount-fork.log 2>&1 &
for i in $(seq 1 60); do sleep 1; mountpoint -q /mnt/c4fork2 && break; done
echo "  mount=$(mountpoint -q /mnt/c4fork2 && echo y || echo n) after ${i}s"
echo "  可见文件数: $(find /mnt/c4fork2/sealbase -type f 2>/dev/null | wc -l)"

echo "=== 8. 写隔离：在 fork 里改文件 + 新建 ==="
if mountpoint -q /mnt/c4fork2; then
  F=$(find /mnt/c4fork2/sealbase -type f 2>/dev/null | head -1)
  [ -n "$F" ] && echo "modified-in-fork" > "$F" && echo "  改文件 OK: $F"
  echo "fork-only" > /mnt/c4fork2/FORK_ONLY.txt 2>&1 && echo "  新建文件 OK"
  ls /mnt/c4fork2/FORK_ONLY.txt 2>/dev/null | sed 's/^/  /'
  umount /mnt/c4fork2 2>/dev/null; echo "  umount fork rc=$?"
fi

echo "=== 9. 重新挂 base 检查隔离 ==="
pkill -f 'mount.*c4seal' 2>/dev/null || true
sleep 2; umount -l /mnt/c4seal 2>/dev/null || true
setsid nohup "$B" mount --privileged --volume-format workspace-v1 \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS \
  --workspace "$WID" --data-backend s3 --s3-bucket c4ws-20261108 \
  --s3-endpoint http://10.20.0.13:9000 --s3-region us-east-1 \
  /mnt/c4seal > $OUT/mount-base2.log 2>&1 &
for i in $(seq 1 60); do sleep 1; mountpoint -q /mnt/c4seal && break; done
if mountpoint -q /mnt/c4seal; then
  echo "  base 文件数: $(find /mnt/c4seal/sealbase -type f 2>/dev/null | wc -l)"
  F=$(find /mnt/c4seal/sealbase -type f 2>/dev/null | head -1)
  [ -n "$F" ] && echo "  base 抽样内容: $(head -c 40 "$F" 2>/dev/null)"
  [ -f /mnt/c4seal/FORK_ONLY.txt ] && echo "  ⚠️ base 里出现 FORK_ONLY.txt（隔离失败）" || echo "  ✅ base 里没有 FORK_ONLY.txt（隔离成立）"
  umount /mnt/c4seal 2>/dev/null
fi
echo "=== DONE ==="
