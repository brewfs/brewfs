#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
V=/srv/ax-data/c3/verify-base-brew-20261107
echo "=== verify-base 进度 ==="
P=$(pgrep -f 'smallfiles verify-base' | head -1)
echo "  pid=${P:-无}  etime=$(ps -o etime= -p ${P:-1} 2>/dev/null | tr -d ' ')"
echo "  目录内容: $(ls -A $V 2>/dev/null | tr '\n' ' ')"
[ -f $V/result.json ] && echo "  result: $(head -c 250 $V/result.json)"
[ -f $V/started.json ] && echo "  started: $(cat $V/started.json)"
echo
echo "=== 挂载是否还能列目录（再抽一次）==="
for d in d0000 d0500; do echo "  $d: $(ls -1 /mnt/brewfs-c3/base/$d 2>/dev/null | wc -l)"; done
echo "  base 顶层: $(ls -1 /mnt/brewfs-c3/base 2>/dev/null | wc -l)"
EOS
