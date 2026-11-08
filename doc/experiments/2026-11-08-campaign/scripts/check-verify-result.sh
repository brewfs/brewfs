#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 内存与进程 ==="
free -g | awk '/Mem:/{print "  used="$3"G avail="$7"G"}'
ps -C brewfs-patched -o pid,etime,pcpu,rss --no-headers 2>/dev/null | awk '{printf "  brewfs pid=%s etime=%s cpu=%s%% rss=%.0fMB\n",$1,$2,$3,$4/1024}'
echo "  verify 进程: $(pgrep -fc 'verify-base' || echo 0)"
echo
echo "=== verify 日志尾 ==="
tail -12 /srv/ax-data/c3/verify-base-brew-20261107.log 2>/dev/null | cut -c1-160 | sed 's/^/  /'
echo
echo "=== verify 输出目录 ==="
ls -d /srv/ax-data/c3/verify-base-brew-* 2>/dev/null | sed 's/^/  /'
for d in /srv/ax-data/c3/verify-base-brew-*/; do
  echo "  [$d] $(ls -A $d 2>/dev/null | tr '\n' ' ')"
  [ -f "$d/result.json" ] && echo "    ✅ $(head -c 250 $d/result.json)"
done
echo
echo "=== 是否有 OOM 痕迹 ==="
dmesg -T 2>/dev/null | grep -i 'out of memory\|oom' | tail -3 | cut -c1-140 | sed 's/^/  /'
EOS
