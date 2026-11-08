#!/bin/bash
# 杀掉仍在跑的 c1e（避免两个 runner 互相污染），只保留 c1h
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 当前 tmux ==="
tmux ls 2>/dev/null | sed 's/^/  /'
tmux kill-session -t c1e 2>/dev/null || true
tmux kill-session -t c1d 2>/dev/null || true
sleep 1
echo "  清理后: $(tmux ls 2>/dev/null | tr '\n' ' ')"
echo
echo "=== c1h 进度 ==="
tail -8 /srv/ax-data/c1h/run.log 2>/dev/null | cut -c1-110 | sed 's/^/  /'
echo
echo "=== results ==="
cat /srv/ax-data/c1h/results.tsv 2>/dev/null | sed 's/^/  /'
echo
echo "=== fio 进程 ==="
pgrep -af /usr/bin/fio | head -2 | cut -c1-100 | sed 's/^/  /'
EOS
