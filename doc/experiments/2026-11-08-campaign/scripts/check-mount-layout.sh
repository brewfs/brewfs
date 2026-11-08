#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 挂载与目录 ==="
echo "  mountpoint: $(mountpoint -q /mnt/brewfs-c3/base && echo y || echo n)"
echo "  base 下: $(ls /mnt/brewfs-c3/base 2>/dev/null | tr '\n' ' ')"
echo "  small 下目录数: $(ls /mnt/brewfs-c3/base/small 2>/dev/null | wc -l)"
echo "  brewfs 进程: $(pgrep -fc brewfs-patched || echo 0)"
echo
echo "=== smallfiles 帮助中的必需参数 ==="
/srv/ax-data/artifacts/bin/smallfiles run --help 2>&1 | head -12 | sed 's/^/  /'
EOS
