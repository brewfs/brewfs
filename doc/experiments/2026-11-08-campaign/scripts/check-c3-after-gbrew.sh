#!/bin/bash
# gbrew 会话消失：检查 C3 侧它的产出与 pilot 是否完成
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== c3 最新产物 ==="
ls -lt /srv/ax-data/c3 | head -12 | awk '{print "  "$6" "$7" "$8"  "$9}'
echo
echo "=== AGENT-REPORT 尾 25 ==="
tail -25 /srv/ax-data/c3/AGENT-REPORT.md 2>/dev/null | cut -c1-150 | sed 's/^/  /'
echo
echo "=== pilot 产物 ==="
ls -d /srv/ax-data/c3/pilot-brew-* 2>/dev/null | sed 's/^/  /'
for d in /srv/ax-data/c3/pilot-brew-*/; do
  echo "  [$d] $(ls -A $d 2>/dev/null | tr '\n' ' ')"
  [ -f "$d/run.out" ] && echo "    run.out: $(head -c 300 $d/run.out)"
done
echo
echo "=== workspace 构建状态 ==="
D=/srv/ax-data/c3/brew-workspace-build-20261107
tail -3 $D/run.log 2>/dev/null | cut -c1-140 | sed 's/^/  /'
echo "  ws-c3 目录: $(ls -1 /mnt/brewfs-c3/base/ws-c3-20261107 2>/dev/null | wc -l)"
echo "  ws-brew 目录: $(ls -1 /mnt/brewfs-c3/ws-brew 2>/dev/null | wc -l)"
EOS
