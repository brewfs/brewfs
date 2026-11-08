#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== c3v2 pilot.tsv ==="
cat /srv/ax-data/c3v2/pilot.tsv 2>/dev/null | sed 's/^/  /' || echo "  无"
echo "=== 文件数 ==="
echo "  $(find /mnt/bfs-c3v/base/small -type f 2>/dev/null | wc -l)"
echo "=== 进程 ==="
echo "  smallfiles=$(pgrep -fc smallfiles || echo 0) mount=$(pgrep -fc 'brewfs-patched mount' || echo 0)"
echo "=== 各 rep 输出尾 ==="
for r in 1 2 3; do
  [ -f /srv/ax-data/c3v2/p$r/run.out ] && { echo "  --- rep$r ---"; tail -3 /srv/ax-data/c3v2/p$r/run.out | cut -c1-130 | sed 's/^/    /'; }
done
EOS
