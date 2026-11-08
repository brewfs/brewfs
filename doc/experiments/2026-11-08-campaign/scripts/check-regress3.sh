#!/bin/bash
set -u
echo "=== $(date '+%H:%M:%S') regress 构建产物检查 ==="
echo "  codex=$(pgrep -fc 'codex exec') rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)"
echo "  mem=$(free -g | awk '/Mem:/{print $7}')G"
echo
echo "--- 二进制 ---"
find /mnt/brewfs-data/home/hxy/regress-20261104/bin -type f 2>/dev/null | while read -r p; do
  SZ=$(stat -c%s "$p" 2>/dev/null || echo 0)
  printf "  %6.0f MB  %s\n" "$(awk "BEGIN{print $SZ/1048576}")" "$p"
done
echo
echo "--- build 日志尾 ---"
tail -3 /mnt/brewfs-data/home/hxy/regress-20261104/build/main-brewfs.log 2>/dev/null | cut -c1-110 | sed 's/^/  /'
echo
echo "--- 所有日志 ---"
ls -lt --time-style=+%H:%M /mnt/brewfs-data/home/hxy/regress-20261104/build/*.log 2>/dev/null | head -5 | awk '{printf "  %s %9s %s\n",$6,$5,$7}'
echo
echo "--- rustc ---"
ps -C rustc -o etime,pcpu,rss --no-headers 2>/dev/null | head -3 | sed 's/^/  /'
