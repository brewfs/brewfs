#!/bin/bash
set -u
D=/mnt/brewfs-data/home/hxy/issue424
L=$D/final.log
echo "=== $(date '+%H:%M:%S') 最终构建状态 ==="
echo "  rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)  mem=$(free -g | awk '/Mem:/{print $7}')G"
echo
echo "--- rustc RSS（关键：应约 10G 以内）---"
ps -C rustc -o etime,pcpu,rss,nlwp --no-headers 2>/dev/null | head -2 | awk '{printf "  etime=%s cpu=%s%% rss=%.1fGB threads=%s\n",$1,$2,$3/1048576,$4}'
echo
echo "--- RUSTFLAGS 是否真的带 codegen-units=4 ---"
P=$(pgrep -x rustc | head -1)
if [ -n "${P:-}" ]; then
  tr '\0' '\n' < /proc/$P/cmdline 2>/dev/null | grep -A1 -E '^-C$' | grep -E 'codegen-units|debuginfo' | head -4 | sed 's/^/  /'
  tr '\0' '\n' < /proc/$P/environ 2>/dev/null | grep -E '^RUSTFLAGS=' | head -1 | sed 's/^/  /'
fi
echo
echo "--- final.log 尾 8 行 ---"
tail -8 "$L" 2>/dev/null | cut -c1-120 | sed 's/^/  /'
echo "  ($(stat -c%s "$L" 2>/dev/null) B)"
