#!/bin/bash
set -u
echo "=== $(date '+%H:%M:%S') 编译是否真在推进（CPU 时间趋势）==="
P=$(pgrep -x rustc | head -1)
if [ -n "${P:-}" ]; then
  T1=$(awk '{print $14+$15}' /proc/$P/stat 2>/dev/null)
  R1=$(awk '/VmRSS/{print $2}' /proc/$P/status 2>/dev/null)
  sleep 20
  T2=$(awk '{print $14+$15}' /proc/$P/stat 2>/dev/null)
  R2=$(awk '/VmRSS/{print $2}' /proc/$P/status 2>/dev/null)
  echo "  pid=$P"
  echo "  CPU ticks: $T1 -> $T2（增量 $((T2-T1))，20 秒内约 $(( (T2-T1)/100 )) 秒 CPU）"
  echo "  RSS: $((R1/1048576)) GB -> $((R2/1048576)) GB"
  echo "  命令行: $(tr '\0' ' ' < /proc/$P/cmdline 2>/dev/null | cut -c1-180)"
else
  echo "  无 rustc"
fi
echo
echo "=== 当前 unit（看是谁在编什么）==="
systemctl list-units 'issue426-*' --no-pager --plain 2>/dev/null | head -3 | cut -c1-200 | sed 's/^/  /'
echo
echo "=== swap 使用 ==="
free -g | sed -n 3p | sed 's/^/  /'
