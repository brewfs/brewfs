#!/bin/bash
# 冷读 v2：每次 prep 前都重挂被测系统（RustFS 重启会破坏既有 mount）
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.161 'bash -s' <<'EOS' 2>&1
set -u
pkill -9 -f c1v2-cold.py 2>/dev/null || true
pkill -9 -f 'fio ' 2>/dev/null || true
sleep 3
rm -f /mnt/jfs-c1v2/*.dat /mnt/bfs-c1v2/*.dat 2>/dev/null || true
echo "  清理完成: fio=$(pgrep -fc 'fio ' || echo 0) py=$(pgrep -fc c1v2-cold || echo 0)"
EOS
