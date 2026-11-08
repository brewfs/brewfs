#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== workspace 构建日志（r16）尾 8 ==="
tail -8 /srv/ax-data/c3/workspace-build-driver-r16.log 2>/dev/null | cut -c1-160 | sed 's/^/  /'
echo
echo "=== 构建产物目录 ==="
D=/srv/ax-data/c3/brew-workspace-build-20261107
ls -A $D 2>/dev/null | head -8 | sed 's/^/  /'
[ -f "$D/summary.json" ] && echo "  summary: $(head -c 300 $D/summary.json)"
echo
echo "=== 当前 ws 状态 ==="
echo "  ws-brew 目录: $(ls -1 /mnt/brewfs-c3/ws-brew 2>/dev/null | wc -l) / 1024"
echo "  mount CPU: $(ps -C brewfs-patched -o pcpu= --no-headers 2>/dev/null | tr -d ' ')"
echo "  RSS: $(ps -C brewfs-patched -o rss= --no-headers 2>/dev/null | awk '{s+=$1} END{printf "%.0fMB", s/1024}')"
echo
echo "=== 构建脚本要点（前 25 行）==="
head -25 /srv/ax-data/c3/build_workspace.sh 2>/dev/null | sed 's/^/  /'
EOS
