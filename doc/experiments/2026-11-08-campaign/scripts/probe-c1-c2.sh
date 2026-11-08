#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== C1 进度 ==="
echo "  进程: $(pgrep -fc c1fio4.py || echo 0)"
echo "  已完成 cell: $(($(wc -l < /srv/ax-data/c1fio/c1.tsv)-1))"
tail -4 /srv/ax-data/c1fio/run.log 2>/dev/null | cut -c1-150 | sed 's/^/  /'
echo
echo "=== C2 材料：replay 工具与 trace ==="
ls -1 /srv/ax-data/c2b 2>/dev/null | head -12 | sed 's/^/  /'
echo "  --- 可执行 ---"
ls -1 /srv/ax-data/artifacts/bin 2>/dev/null | grep -iE 'trace|replay|fbtrace' | sed 's/^/  /'
echo "  --- trace 数据 ---"
find /srv/ax-data -maxdepth 3 -iname '*trace*' -o -maxdepth 3 -iname '*pytest*' -o -maxdepth 3 -iname '*django*' 2>/dev/null | head -10 | sed 's/^/  /'
EOS
