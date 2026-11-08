#!/bin/bash
set -u
D=/mnt/brewfs-data/home/hxy/issue-426-20261105
echo "=== $(date '+%H:%M:%S') #426 复测准备状态 ==="
echo "--- 目录结构 ---"
ls -lt "$D" 2>/dev/null | head -8 | sed 's/^/  /'
echo
echo "--- audit/build 下的产物 ---"
find "$D/audit/build" -maxdepth 2 -name '*.exit' -o -maxdepth 2 -name '*.log' 2>/dev/null | head -8 | sed 's/^/  /'
echo
echo "--- 当前编译 unit ---"
systemctl list-units 'issue426-*' --no-pager --plain 2>/dev/null | head -3 | cut -c1-190 | sed 's/^/  /'
echo
echo "--- rustc 命令行（编哪个 target）---"
P=$(pgrep -x rustc | head -1)
[ -n "${P:-}" ] && tr '\0' ' ' < /proc/$P/cmdline 2>/dev/null | grep -oE '\-\-(bin|lib|crate-name) [a-z_]+' | head -3 | sed 's/^/  /'
[ -n "${P:-}" ] && tr '\0' ' ' < /proc/$P/cmdline 2>/dev/null | grep -oE 'profile=[a-z]+|opt-level=[0-9]|debuginfo=[0-9a-z-]+' | head -4 | sed 's/^/  /'
echo
echo "--- 已有二进制 ---"
find "$D" -name 'brewfs' -type f -newermt '-6 hours' 2>/dev/null | head -5 | while read -r f; do
  printf "  %6.0f MB  %s\n" "$(awk "BEGIN{print $(stat -c%s "$f")/1048576}")" "$f"
done
