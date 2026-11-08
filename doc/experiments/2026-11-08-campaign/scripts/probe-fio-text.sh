#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== fio 版本 ==="
fio --version 2>&1 | sed 's/^/  /'
echo "=== 文本输出里的 clat 百分位表 ==="
D=/mnt/jfs-c1fio; rm -f $D/tx.dat
fio --name=tx --directory=$D --filename=tx.dat --rw=randwrite --bs=1M --size=256M \
    --direct=1 --ioengine=libaio --iodepth=16 --numjobs=1 --end_fsync=1 \
    --percentile_list=50,95,99 2>&1 | grep -aA6 'clat percentiles' | head -10 | sed 's/^/  /'
rm -f $D/tx.dat
EOS
