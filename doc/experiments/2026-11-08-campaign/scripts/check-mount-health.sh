#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== obj 状态 ==="
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.163 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
echo "rustfs=$(pgrep -x rustfs | wc -l) health=$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://127.0.0.1:9000/health)"
tail -3 /srv/ax-data/logs/rustfs.log 2>/dev/null | cut -c1-140
EOS
echo "=== client 挂载状态与日志 ==="
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.161 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
echo "juice=$(mountpoint -q /mnt/jfs-c1v2 && echo y || echo n) brew=$(mountpoint -q /mnt/bfs-c1v2 && echo y || echo n)"
echo "juice 进程=$(pgrep -fc 'juicefs mount' || echo 0) brew 进程=$(pgrep -fc 'brewfs-patched mount' || echo 0)"
echo "--- juice mount 日志尾 ---"; tail -4 /srv/ax-data/logs/juice-mount.log 2>/dev/null | cut -c1-150
echo "--- brew mount 日志尾 ---"; tail -4 /srv/ax-data/logs/brew-mount.log 2>/dev/null | cut -c1-150
EOS
