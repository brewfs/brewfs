#!/bin/bash
set -u
D=/mnt/brewfs-data/home/hxy/issue424
echo "=== $(date '+%H:%M:%S') direct 构建进度 ==="
echo "  rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0) mem=$(free -g | awk '/Mem:/{print $7}')G"
ps -C rustc -o etime,pcpu,rss --no-headers 2>/dev/null | head -2 | awk '{printf "  rustc: etime=%s cpu=%s%% rss=%.1fGB\n",$1,$2,$3/1048576}'
echo
echo "--- build-direct.log：已编译 crate 数 ---"
grep -ac 'Compiling ' "$D/build-direct.log" 2>/dev/null | sed 's/^/  /'
echo "--- 最近 4 行 ---"
tail -4 "$D/build-direct.log" 2>/dev/null | cut -c1-110 | sed 's/^/  /'
echo
echo "--- 有无 error ---"
grep -acE '^error' "$D/build-direct.log" 2>/dev/null | sed 's/^/  error 行数: /'
echo
echo "--- direct.log 尾 4 行 ---"
tail -4 "$D/direct.log" 2>/dev/null | cut -c1-110 | sed 's/^/  /'
