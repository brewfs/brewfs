#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== a-obj 磁盘 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.13 'df -h /srv/ax-data | tail -1 | sed "s/^/  /"' 2>/dev/null
echo
echo "=== a-client JuiceFS 状态 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  juicefs 挂载数: $(mount | grep -c juicefs)"
echo "  juicefs 进程: $(pgrep -fc 'juicefs' || echo 0)"
echo "  c1j2 目录: $(ls /srv/ax-data/c1j2 2>/dev/null | tr '\n' ' ')"
echo "  c1j2 raw: $(ls /srv/ax-data/c1j2/raw/*.json 2>/dev/null | wc -l)"
echo "  fio: $(pgrep -fc '/usr/bin/fio' || echo 0)"
echo "  内存: $(free -g | awk '/Mem:/{print $3"G/"$2"G"}')"
EOS
echo
echo "=== gc1j 日志尾 ==="
tail -4 /root/gc1j.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
