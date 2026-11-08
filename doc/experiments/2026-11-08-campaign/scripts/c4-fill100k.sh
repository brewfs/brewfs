#!/bin/bash
# 往里写 10 万文件（100 目录 × 976），并计时
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
SRC=/mnt/brewfs-c3/base/small
DST=/mnt/c4ws/base
echo "=== 1. 准备目标 ==="
mkdir -p $DST
echo "  src 目录数: $(ls $SRC | wc -l)"
echo
echo "=== 2. 复制 103 个目录（约 10 万文件）并计时 ==="
T0=$(date +%s)
for d in $(ls $SRC | head -103); do
  cp -a "$SRC/$d" "$DST/$d" 2>/dev/null || echo "  ⚠️ $d 失败"
done
T1=$(date +%s)
N=$(find $DST -type f | wc -l)
echo "  files=$N  elapsed=$((T1-T0))s  rate=$((N/(T1-T0+1))) files/s"
echo
echo "=== 3. 抽查 ==="
D=$(ls $DST | head -1); F=$(ls $DST/$D | head -1)
echo "  $D/$F size=$(stat -c%s $DST/$D/$F 2>/dev/null)"
head -c 8 $DST/$D/$F 2>/dev/null | od -c | head -1 | sed 's/^/  content: /'
EOS
