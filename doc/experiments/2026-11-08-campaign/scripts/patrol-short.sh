#!/bin/bash
set -u
echo "=== tmux ==="
tmux ls 2>/dev/null | sed 's/^/  /'
for s in gbrew gc1; do
  echo "  $s log: $(stat -c%s /root/$s.log 2>/dev/null || echo 0) B"
done
echo
echo "=== C2 rust ==="
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=8 root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
tail -1 /srv/ax-data/c1c2/c2/results/uid1000-rust.out 2>/dev/null | sed 's/^/  /'
echo "  json:"
ls -l /srv/ax-data/c1c2/c2/results/uid1000/*.json 2>/dev/null | awk '{print "    "$5" "$9}'
echo "  replay 进程: $(pgrep -fc full-fuse-replay || echo 0)"
echo
echo "=== c1c2 目录 ==="
ls /srv/ax-data/c1c2 2>/dev/null | tr '\n' ' ' | sed 's/^/  /'
echo
echo "=== METRICS 文件 ==="
ls -l /srv/ax-data/c1c2/METRICS-C1C2.md 2>/dev/null | sed 's/^/  /'
EOS
