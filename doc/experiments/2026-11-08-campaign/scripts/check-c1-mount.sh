#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== c1 mount log 尾 ==="
tail -6 /srv/ax-data/c1c2/c1/logs/c1-brewfs-mount.log 2>/dev/null | cut -c1-140 | sed 's/^/  /'
echo
echo "=== 当前挂载 ==="
mount | grep -E 'brewfs|juicefs' | cut -c1-110 | sed 's/^/  /'
echo
echo "=== brewfs/juicefs 进程 ==="
pgrep -af 'brewfs-patched|juicefs mount' | head -4 | cut -c1-100 | sed 's/^/  /'
echo
echo "=== c1 构建脚本 ==="
ls -lt /srv/ax-data/c1c2/c1/scripts 2>/dev/null | head -5 | awk '{print "  "$6" "$7" "$8"  "$9}'
echo
echo "=== 是否有 c1 卷目录 ==="
ls /mnt/brewfs-c1 /mnt/juicefs-c1 2>/dev/null | head -6 | sed 's/^/  /'
EOS
