#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 防冲突：停掉等待中的 chain（它会误在冷读期间启动 C2）==="
pkill -f '/root/chain.sh' 2>/dev/null || true
sleep 1
echo "  chain: $(pgrep -fc chain.sh || echo 0)  c1cold: $(pgrep -fc c1cold.py || echo 0)"
echo
echo "=== 冷读进度 ==="
echo "  coldread cells: $(grep -c coldread /srv/ax-data/c1fio/c1.tsv 2>/dev/null || echo 0)/18"
tail -2 /srv/ax-data/c1fio/run.log 2>/dev/null | cut -c1-170 | sed 's/^/  /'
EOS
