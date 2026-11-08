#!/bin/bash
set -u
G=/mnt/brewfs-data/home/hxy/issue-426-20261105/gates
echo "=== $(date '+%H:%M:%S') 426 门禁汇总 ==="
echo "--- 所有门禁的 exit code ---"
for f in "$G"/*.exit; do
  [ -f "$f" ] || continue
  printf "  %-28s %s\n" "$(basename "$f" .exit)" "$(cat "$f" 2>/dev/null)"
done
echo
echo "--- 最近 5 个日志的关键结果 ---"
for f in $(ls -t "$G"/*.log 2>/dev/null | head -5); do
  echo "  === $(basename "$f") ==="
  grep -aE '^test result:|FAILED|error\[|error:' "$f" 2>/dev/null | tail -3 | cut -c1-140 | sed 's/^/    /'
done
echo
echo "--- 进程 ---"
echo "  codex=$(pgrep -fc 'codex exec' || echo 0) rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0) mem=$(free -g | awk '/Mem:/{print $7}')G"
