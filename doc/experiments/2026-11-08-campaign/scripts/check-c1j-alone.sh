#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== JuiceFS lane 当前状态（gc2 已停，应独跑）==="
echo "  fio: $(pgrep -fc 'fio ' || echo 0)"
ps -o etime=,args= -p $(pgrep -d, -f 'fio --name' 2>/dev/null) 2>/dev/null | head -2 | cut -c1-110 | sed 's/^/    /'
D=/mnt/juicefs-c1/c1-data/c1j
echo "  目录用量: $(du -sh $D 2>/dev/null | cut -f1)"
ls -lt $D 2>/dev/null | head -3 | awk '{print "    "$6" "$7" "$8"  "$9}'
echo "  采样 15 s:"
a=$(du -sm $D 2>/dev/null | cut -f1); sleep 15; b=$(du -sm $D 2>/dev/null | cut -f1)
echo "    ${a}MB -> ${b}MB ($((b-a)) MB/15s ≈ $(( (b-a)/15 )) MB/s)"
echo
echo "=== juicefs 客户端统计 ==="
ls /sys/fs/juicefs 2>/dev/null | head -2
cat /sys/fs/juicefs/*/stats 2>/dev/null | head -6 | sed 's/^/  /'
EOS
