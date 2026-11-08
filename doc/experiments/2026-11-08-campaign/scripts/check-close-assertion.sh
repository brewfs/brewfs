#!/bin/bash
set -u
G=/mnt/brewfs-data/home/hxy/issue-426-20261105/gates
echo "=== $(date '+%H:%M:%S') directory-close-assertion 的真实报错 ==="
L="$G/directory-close-assertion.log"
if [ -f "$L" ]; then
  grep -aE 'panicked|assertion|left|right|test result|FAILED|error\[|error:' "$L" 2>/dev/null | head -12 | cut -c1-170 | sed 's/^/  /'
  echo "  --- 尾 10 行 ---"
  tail -10 "$L" | cut -c1-160 | sed 's/^/  /'
else
  echo "  （无该日志）"
fi
echo
echo "=== 该门禁的命令 ==="
systemctl cat issue426-directory-close-assertion.service 2>/dev/null | grep -E 'ExecStart|Description' | head -3 | cut -c1-200 | sed 's/^/  /'
echo
echo "=== 进程 ==="
echo "  codex=$(pgrep -fc 'codex exec' || echo 0) rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)"
echo
echo "=== agent 日志尾（看它在想什么）==="
grep -av '^$' /home/hxy/logs/g426.log 2>/dev/null | tail -12 | cut -c1-150 | sed 's/^/  /'
