#!/bin/bash
# 在 build 机上验证 seal → fork → 可见性 → 写隔离（sqlite + local-fs）
set -u
K=/root/.ssh/ax-c1-20261106
B=10.20.2.164
ssh -o StrictHostKeyChecking=no -i $K root@$B 'bash -s' <<'EOS' 2>&1
set -u
cd /srv/ax-data/src || exit 1
BIN=target/release/brewfs
OUT=/srv/ax-data/sealtest
rm -rf $OUT && mkdir -p $OUT/data $OUT/mnt
echo "=== 0. 二进制 ==="
ls -lh $BIN 2>/dev/null | awk '{print "  "$5, $6, $7, $8}' || { echo "  无二进制"; exit 1; }
echo "  seal 帮助:"; $BIN workspace seal --help 2>&1 | head -8 | sed 's/^/    /'
cat > $OUT/data.yaml <<'YAML'
data:
  backend: local-fs
  dir: /srv/ax-data/sealtest/data
YAML
echo "=== 1. init-volume ==="
META="sqlite://$OUT/meta.db"
$BIN workspace init-volume --meta-backend sqlx --meta-url "$META" --data-config $OUT/data.yaml 2>&1 | tail -4 | cut -c1-140 | sed 's/^/  /'
echo "=== 2. list ==="
LIST=$($BIN workspace list --meta-backend sqlx --meta-url "$META" 2>/dev/null)
echo "$LIST" | head -6 | cut -c1-140 | sed 's/^/  /'
WID=$(echo "$LIST" | grep -aoE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)
echo "  WID=$WID"
[ -z "$WID" ] && { echo "  没有 workspace id"; exit 2; }
echo "=== 3. 挂载并写 20 文件 ==="
$BIN mount --meta-backend sqlx --meta-url "$META" --data-backend local-fs --data-dir $OUT/data $OUT/mnt > $OUT/mount.log 2>&1 &
for i in $(seq 1 40); do sleep 1; mountpoint -q $OUT/mnt && break; done
echo "  mount=$(mountpoint -q $OUT/mnt && echo y || echo n) after ${i}s"
if [ "$(mountpoint -q $OUT/mnt && echo y || echo n)" = "y" ]; then
  mkdir -p $OUT/mnt/sealbase
  for i in $(seq 1 20); do echo "content-$i" > $OUT/mnt/sealbase/f$i.txt; done
  echo "  files=$(ls $OUT/mnt/sealbase | wc -l) sample=$(cat $OUT/mnt/sealbase/f1.txt)"
  umount $OUT/mnt; echo "  umount rc=$?"
else
  tail -8 $OUT/mount.log | cut -c1-140 | sed 's/^/  /'; exit 3
fi
echo "=== 4. seal ==="
T0=$(date +%s%N); SEAL=$($BIN workspace seal --meta-backend sqlx --meta-url "$META" "$WID" 2>&1); RC=$?; T1=$(date +%s%N)
echo "  rc=$RC wall=$(( (T1-T0)/1000000 ))ms"
echo "$SEAL" | tail -10 | cut -c1-170 | sed 's/^/  /'
REV=$(echo "$SEAL" | grep -aoE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:[0-9]+:[0-9a-f]+' | head -1)
echo "  REV=${REV:0:70}"
[ -z "$REV" ] && { echo "  未解析到 revision"; exit 4; }
echo "=== 5. fork ==="
T0=$(date +%s%N)
$BIN workspace fork "$REV" --count 1 --data-config $OUT/data.yaml --meta-backend sqlx --meta-url "$META" > $OUT/fork.json 2>&1
RC=$?; T1=$(date +%s%N)
echo "  rc=$RC wall=$(( (T1-T0)/1000000 ))ms"
tail -4 $OUT/fork.json | cut -c1-140 | sed 's/^/  /'
FID=$(grep -aoE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' $OUT/fork.json | head -1)
echo "  FID=$FID"
if [ -n "$FID" ]; then
  echo "=== 6. 挂 fork 验证可见性 ==="
  $BIN mount --meta-backend sqlx --meta-url "$META" --data-backend local-fs --data-dir $OUT/data --workspace "$FID" $OUT/mnt > $OUT/mount-fork.log 2>&1 &
  for i in $(seq 1 40); do sleep 1; mountpoint -q $OUT/mnt && break; done
  echo "  mount=$(mountpoint -q $OUT/mnt && echo y || echo n)"
  echo "  fork 内文件数=$(ls $OUT/mnt/sealbase 2>/dev/null | wc -l) 抽样=$(cat $OUT/mnt/sealbase/f1.txt 2>/dev/null)"
  echo "=== 7. 写隔离 ==="
  echo "fork-only" > $OUT/mnt/FORK_ONLY.txt 2>&1 && echo "  写 FORK_ONLY.txt OK"
  umount $OUT/mnt 2>/dev/null; echo "  umount fork rc=$?"
  echo "=== 8. 重挂 base 检查隔离 ==="
  $BIN mount --meta-backend sqlx --meta-url "$META" --data-backend local-fs --data-dir $OUT/data --workspace "$WID" $OUT/mnt > $OUT/mount-base2.log 2>&1 &
  for i in $(seq 1 40); do sleep 1; mountpoint -q $OUT/mnt && break; done
  if mountpoint -q $OUT/mnt; then
    echo "  base 文件数=$(ls $OUT/mnt/sealbase 2>/dev/null | wc -l)"
    [ -f $OUT/mnt/FORK_ONLY.txt ] && echo "  ⚠️ base 出现 FORK_ONLY.txt（隔离失败）" || echo "  ✅ base 无 FORK_ONLY.txt（隔离成立）"
    umount $OUT/mnt 2>/dev/null
  fi
fi
echo "=== DONE ==="
EOS
