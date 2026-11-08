#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== 两条 lane 的进程与最新文件 ==="
for h in 10.20.0.11 10.20.0.12; do
  echo "--- $h ---"
  ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=6 root@$h 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  fio: $(pgrep -fc 'fio ' || echo 0)"
ps -o etime,args -p $(pgrep -d, -f 'fio --name' 2>/dev/null) 2>/dev/null | tail -2 | cut -c1-100 | sed 's/^/    /'
D1=/srv/ax-data/c1m/raw; D2=/srv/ax-data/c1j/raw; D3=/srv/ax-data/c1c2/c1/raw
for D in $D1 $D2 $D3; do
  [ -d "$D" ] || continue
  echo "  [$D] $(ls $D/*.fio.json 2>/dev/null | wc -l) raw"
  ls -t $D 2>/dev/null | head -2 | sed 's/^/    /'
done
EOS
done
