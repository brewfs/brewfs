#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
D=/mnt/jfs-c1fio; rm -f $D/tx.dat
for LIST in '50,95,99' '50:95:99'; do
  echo "=== percentile_list=$LIST ==="
  fio --name=tx --directory=$D --filename=tx.dat --rw=randwrite --bs=1M --size=256M \
      --direct=1 --ioengine=libaio --iodepth=16 --numjobs=1 --end_fsync=1 \
      --percentile_list=$LIST 2>&1 | grep -aA5 'clat percentiles' | head -7 | sed 's/^/  /'
  rm -f $D/tx.dat
done
EOS
