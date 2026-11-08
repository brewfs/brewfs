#!/bin/bash
# 检查三台实验机是否有实际负载（CPU/进程/网络），用于判断是否空闲可停
set -u
K=/root/.ssh/ax-c1-20261106
for h in 10.20.0.11 10.20.0.12 10.20.0.13; do
  echo "===== $h ====="
  ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=6 root@$h 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  load: $(uptime | sed 's/.*load average: //')"
echo "  cpu 空闲%: $(top -bn1 | grep -E '^%Cpu' | awk '{print $8}' | head -1)"
echo "  内存: $(free -g | awk '/Mem:/{print $3"G/"$2"G"}')"
echo "  关键进程:"
ps -eo pcpu,rss,comm --sort=-pcpu 2>/dev/null | head -5 | tail -4 | awk '{printf "    %-16s cpu=%s%% rss=%.0fMB\n", $3, $1, $2/1024}'
echo "  网络累计: $(cat /proc/net/dev | awk '/eth0/{printf "rx=%.1fMB tx=%.1fMB", $2/1048576, $10/1048576}')"
EOS
  echo
done
