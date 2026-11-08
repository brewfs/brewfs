#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== jump ==="
tmux ls 2>/dev/null | sed 's/^/  /'
echo "  gc1 log: $(stat -c%s /root/gc1.log 2>/dev/null) B"
echo
echo "=== a-client 资源与 fio ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "  内存: $(free -g | awk '/Mem:/{print $3"G/"$2"G"}')"
echo "  磁盘: $(df -h /srv/ax-data | tail -1 | awk '{print $3"/"$2" ("$5")"}')"
echo "  fio 进程: $(pgrep -fc 'fio ' || echo 0)"
echo "  c1 raw 最新:"
ls -lt /srv/ax-data/c1c2/c1/raw 2>/dev/null | head -4 | awk '{print "    "$6" "$7" "$8"  "$9}'
echo "  c1 目录: $(ls /srv/ax-data/c1c2/c1 2>/dev/null | tr '\n' ' ')"
EOS
echo
echo "=== 三台机 CPU ==="
for h in 10.20.0.11 10.20.0.12 10.20.0.13; do
  ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=6 root@$h "echo '  $h: '$(uptime | sed 's/.*load average: //')" 2>/dev/null
done
