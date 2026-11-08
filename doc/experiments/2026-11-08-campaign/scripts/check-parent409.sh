#!/bin/bash
set -u
echo "=== $(date '+%H:%M:%S') parent409 详细状态 ==="
echo "  rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)  mem=$(free -g | awk '/Mem:/{print $7}')G"
echo
echo "--- rustc 明细（etime/cpu/rss）---"
ps -C rustc -o etime,pcpu,rss --no-headers 2>/dev/null | head -3 | sed 's/^/  /'
echo
echo "--- cgroup ---"
for u in $(systemctl list-units 'cargo-*' --no-pager --plain 2>/dev/null | awk '/cargo-/{print $1}'); do
  echo "  $u"
  systemctl show "$u" -p MemoryCurrent -p MemoryMax -p ActiveState --no-pager 2>/dev/null | sed 's/^/    /'
done
echo
echo "--- 是否有 OOM 击杀记录 ---"
journalctl --since '-70min' --no-pager 2>/dev/null | grep -iE 'oom-kill|killed process' | tail -3 | sed 's/^/  /' || echo "  无"
echo
echo "--- build 日志最后 5 行 ---"
tail -5 /mnt/brewfs-data/home/hxy/regress-20261104/build/parent409-brewfs.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
echo
echo "--- 日志大小（判断是否在推进）---"
stat -c '  %s B  %y' /mnt/brewfs-data/home/hxy/regress-20261104/build/parent409-brewfs.log 2>/dev/null
