#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== tmux ==="
tmux ls 2>/dev/null | sed 's/^/  /'
for s in gbrew gc1 grust; do
  echo "  $s log: $(stat -c%s /root/$s.log 2>/dev/null || echo 0) B"
done
echo
echo "=== a-client ==="
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=8 root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  C1 raw 最新:"
ls -lt /srv/ax-data/c1c2/c1/raw 2>/dev/null | head -3 | awk '{print "    "$6" "$7" "$8"  "$9}'
echo "  C1 fio 进程: $(pgrep -fc 'fio ' || echo 0)"
echo "  C1 seq-1m 用量: $(du -sh /mnt/juicefs-c1/c1-data/seq-1m 2>/dev/null | cut -f1)"
echo "  c2b 目录: $(ls /srv/ax-data/c2b 2>/dev/null | tr '\n' ' ')"
echo "  c2b replay 进程: $(pgrep -fc 'full-fuse-replay' || echo 0)"
echo "  内存: $(free -g | awk '/Mem:/{print $3"G/"$2"G"}')  磁盘: $(df -h /srv/ax-data | tail -1 | awk '{print $3"/"$2}')"
EOS
