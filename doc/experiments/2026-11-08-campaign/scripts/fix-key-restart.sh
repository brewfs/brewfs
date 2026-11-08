#!/bin/bash
# 把 ssh key 从 jump 发到 a-client（脚本需要跨机查 a-obj 磁盘），然后重启矩阵
set -u
K=/root/.ssh/ax-c1-20261106
mkdir -p /root/.ssh && chmod 700 /root/.ssh 2>/dev/null
scp -q -o StrictHostKeyChecking=no -i $K $K root@10.20.0.11:/root/.ssh/ax-c1-20261106 2>/dev/null || true
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
chmod 600 /root/.ssh/ax-c1-20261106 2>/dev/null
echo "  key: $(ls -l /root/.ssh/ax-c1-20261106 2>/dev/null | awk '{print $1, $5}')"
echo "  连通性: $(ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -i /root/.ssh/ax-c1-20261106 root@10.20.0.13 'echo ok' 2>&1 | tail -1)"
echo
echo "=== 重启矩阵 ==="
tmux kill-session -t c1run 2>/dev/null || true
rm -rf /srv/ax-data/c1k/raw/* /srv/ax-data/c1k/results.tsv /srv/ax-data/c1k/run.log 2>/dev/null
tmux new-session -d -s c1run "/root/c1-run.sh /srv/ax-data/c1k 2>&1 | tee -a /srv/ax-data/c1k/console.log"
sleep 100
echo "  tmux: $(tmux ls 2>/dev/null | tr '\n' ' ')"
tail -14 /srv/ax-data/c1k/run.log 2>/dev/null | cut -c1-130 | sed 's/^/    /'
EOS
