#!/bin/bash
# 在 a-client 上以 tmux 启动 C1 矩阵（绕开 PowerShell 引号问题）
set -u
K=/root/.ssh/ax-c1-20261106
scp -q -o StrictHostKeyChecking=no -i $K /root/c1-run.sh root@10.20.0.11:/root/c1-run.sh
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
chmod +x /root/c1-run.sh
mkdir -p /srv/ax-data/c1k
tmux kill-session -t c1run 2>/dev/null || true
tmux new-session -d -s c1run "/root/c1-run.sh /srv/ax-data/c1k 2>&1 | tee -a /srv/ax-data/c1k/console.log"
sleep 50
echo "  tmux: $(tmux ls 2>/dev/null | tr '\n' ' ')"
echo "  --- run.log ---"
tail -8 /srv/ax-data/c1k/run.log 2>/dev/null | cut -c1-120 | sed 's/^/    /'
echo "  --- 卷用量: $(du -sh /mnt/juicefs-c1 2>/dev/null | cut -f1)"
EOS
