#!/bin/bash
# 判断 rustc 是否还在推进：看 target 目录增长 + 进程 CPU 时间
set -u
T=/home/hxy/brewfs-pkv-target
PID=$(pgrep -x rustc | head -1)
echo "=== $(date '+%H:%M:%S') rustc 是否在推进 ==="
if [ -n "${PID:-}" ]; then
  echo "  pid=$PID"
  ps -o etime,times,pcpu,rss,nlwp -p "$PID" --no-headers 2>/dev/null | sed 's/^/  /'
  echo "  CPU 时间（秒，两次采样）:"
  T1=$(awk '{print $14+$15}' /proc/$PID/stat 2>/dev/null)
  sleep 20
  T2=$(awk '{print $14+$15}' /proc/$PID/stat 2>/dev/null)
  echo "    ${T1} -> ${T2}（增量 $(awk "BEGIN{print $T2-$T1}") ticks）"
  echo "  RSS: $(awk '/VmRSS/{print $2/1048576" GB"}' /proc/$PID/status 2>/dev/null)"
else
  echo "  （无 rustc）"
fi
echo
echo "=== target 增长（两次采样）==="
S1=$(du -sm "$T" 2>/dev/null | awk '{print $1}')
sleep 15
S2=$(du -sm "$T" 2>/dev/null | awk '{print $1}')
echo "  ${S1} MB -> ${S2} MB（增量 $((S2-S1)) MB）"
echo
echo "=== 最近生成的目标文件 ==="
find "$T" -newermt '-5 minutes' -name '*.o' 2>/dev/null | head -3 | sed 's/^/  /'
