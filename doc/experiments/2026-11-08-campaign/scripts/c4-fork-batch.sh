#!/bin/bash
# C4 第三组：fork --count 10 / 100 计时
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
REV=$(cat /tmp/c4-rev.txt)
OUT=/srv/ax-data/c4ws; mkdir -p $OUT
: > $OUT/fork.tsv
printf "count\tseconds\tper_fork_ms\tok\n" >> $OUT/fork.tsv

for C in 1 10 100; do
  T0=$(date +%s%N)
  LOG=$OUT/fork-$C.log
  timeout 300 $B workspace fork "$REV" --count $C --data-config $OUT/data.yaml \
    --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS \
    --owner c4f > $LOG 2>&1
  RC=$?
  T1=$(date +%s%N)
  MS=$(( (T1-T0)/1000000 ))
  SEC=$(python3 -c "print(f'{$MS/1000:.3f}')")
  PER=$(python3 -c "print(f'{$MS/$C:.2f}')")
  N=$(grep -ac 'workspace_id' $LOG 2>/dev/null || echo 0)
  echo "  count=$C  ${MS} ms  (${PER} ms/fork)  rc=$RC  new_ws=$N" | tee -a $OUT/fork.log
  printf "%s\t%s\t%s\t%s\n" "$C" "$SEC" "$PER" "$N" >> $OUT/fork.tsv
done
echo
echo "=== 汇总 ==="
cat $OUT/fork.tsv | sed 's/^/  /'
echo
echo "=== 当前 workspace 总数 ==="
timeout 90 $B workspace list --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS 2>/dev/null | grep -ac 'workspace_id' | sed 's/^/  /'
EOS
