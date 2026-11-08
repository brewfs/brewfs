#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=8 root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  ws 目录: $(ls -1 /mnt/brewfs-c3/ws-brew 2>/dev/null | wc -l) / 1024"
echo "  ws 文件: $(find /mnt/brewfs-c3/ws-brew -type f 2>/dev/null | wc -l)"
echo "  物化进程: $(pgrep -fc materialize-ws || echo 0)"
echo "  RSS: $(ps -C brewfs-patched -o rss= --no-headers 2>/dev/null | awk '{s+=$1} END{printf "%.0fMB", s/1024}')"
echo "  挂载: $(mountpoint -q /mnt/brewfs-c3/base && echo y || echo n)"
echo "  日志尾: $(tail -2 /srv/ax-data/c3/materialize-ws.log 2>/dev/null | tr '\n' ' ' | cut -c1-160)"
EOS
echo
timeout 40 bash /root/probe-paths.sh 2>&1 | head -3
