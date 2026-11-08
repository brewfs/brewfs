#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== brewfs 进程（可能有孤儿）==="
ps -eo pid,etime,rss,args --sort=etime 2>/dev/null | grep 'brewfs-patched mount' | grep -v grep | cut -c1-140 | sed 's/^/  /'
echo
echo "=== 挂载点 ==="
grep brewfs /proc/mounts | sed 's/^/  /'
echo
echo "=== verify 进程 ==="
pgrep -af 'smallfiles verify-base' | cut -c1-130 | sed 's/^/  /'
echo
echo "=== AGENT-REPORT 尾 12 ==="
tail -12 /srv/ax-data/c3/AGENT-REPORT.md 2>/dev/null | cut -c1-150 | sed 's/^/  /'
EOS
