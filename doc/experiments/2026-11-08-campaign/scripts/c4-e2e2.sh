#!/bin/bash
# /tmp/c4-rev.txt 丢了（重启），从 list 重新生成 revision，再测端到端
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
B=/srv/ax-data/src/brewfs-new/target/release/brewfs
PD=http://10.20.0.12:2379
NS=c4ws
AK=$(cat /etc/ax-c1/rustfs-access); SK=$(cat /etc/ax-c1/rustfs-secret)
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
echo "=== 重建 revision ==="
timeout 120 $B workspace list --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS 2>/dev/null | python3 /root/mkrev.py | sed 's/^/  /'
REV=$(cat /tmp/c4-rev.txt)
echo "  rev_len=${#REV}"
OUT=/srv/ax-data/c4ws
echo "=== fork ==="
T0=$(date +%s%N)
timeout 120 $B workspace fork "$REV" --count 1 --data-config $OUT/data.yaml \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS --owner c4e > $OUT/fork-e2e.json 2>&1
T1=$(date +%s%N)
FORK_MS=$(( (T1-T0)/1000000 ))
NEWID=$(grep -aoE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' $OUT/fork-e2e.json | head -1)
echo "  fork=${FORK_MS}ms new=$NEWID"
[ -z "$NEWID" ] && { tail -6 $OUT/fork-e2e.json | cut -c1-140 | sed 's/^/  /'; exit 1; }
echo "=== mount ==="
mkdir -p /mnt/c4e2e
pkill -f 'brewfs mount.*c4e2e' 2>/dev/null; umount -l /mnt/c4e2e 2>/dev/null || true
T0=$(date +%s%N)
setsid nohup $B mount --privileged --volume-format workspace-v1 \
  --meta-backend tikv --meta-tikv-pd-endpoints $PD --workspace-namespace $NS \
  --workspace "$NEWID" --data-backend s3 --s3-bucket c4ws-20261108 \
  --s3-endpoint http://10.20.0.13:9000 --s3-region us-east-1 /mnt/c4e2e > $OUT/mount-e2e.log 2>&1 &
for i in $(seq 1 900); do mountpoint -q /mnt/c4e2e && break; sleep 0.1; done
T1=$(date +%s%N)
MOUNT_MS=$(( (T1-T0)/1000000 ))
echo "  mount=${MOUNT_MS}ms ready=$(mountpoint -q /mnt/c4e2e && echo y || echo n)"
T0=$(date +%s%N); ls -a /mnt/c4e2e >/dev/null 2>&1; T1=$(date +%s%N)
echo "  first_readdir=$(( (T1-T0)/1000000 ))ms"
echo "  E2E(fork+mount)=$(( FORK_MS + MOUNT_MS ))ms"
umount /mnt/c4e2e 2>/dev/null || true
EOS
