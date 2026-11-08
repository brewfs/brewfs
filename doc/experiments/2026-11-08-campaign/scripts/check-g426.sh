#!/bin/bash
set -u
L=/home/hxy/logs/g426.log
echo "=== $(date '+%H:%M:%S') g426 agent 状态 ==="
echo "  codex=$(pgrep -fc 'codex exec' || echo 0) rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)"
echo "  mem=$(free -g | awk '/Mem:/{print $7}')G"
echo
echo "--- 日志（双采样）---"
stat -c '  采样1: %s B  %y' "$L" 2>/dev/null
sleep 8
stat -c '  采样2: %s B  %y' "$L" 2>/dev/null
echo
echo "--- 最近 25 行（去空行）---"
grep -av '^$' "$L" 2>/dev/null | tail -25 | cut -c1-150 | sed 's/^/  /'
