#!/bin/bash
set -u
G=/mnt/brewfs-data/home/hxy/issue-426-20261105/gates
echo "=== $(date '+%H:%M:%S') 当前正在跑的门禁 ==="
systemctl list-units 'issue426-*' --all --no-pager --plain 2>/dev/null | head -5 | sed 's/^/  /'
echo
echo "--- 该门禁日志尾部（test-overlay-metadata）---"
tail -6 "$G/test-overlay-metadata.log" 2>/dev/null | cut -c1-150 | sed 's/^/  /'
echo
echo "--- 该日志里是否已有 test result ---"
grep -aE '^test result:|running [0-9]+ tests' "$G/test-overlay-metadata.log" 2>/dev/null | tail -3 | sed 's/^/  /'
echo
echo "--- 日志大小/时间 ---"
stat -c '  %s B  %y' "$G/test-overlay-metadata.log" 2>/dev/null
echo
echo "--- rustc 明细 ---"
ps -C rustc -o etime,pcpu,rss --no-headers 2>/dev/null | head -2 | sed 's/^/  /'
echo
echo "--- git 状态 ---"
cd /home/hxy/brewfs-main || exit 1
git status --porcelain | head -6 | sed 's/^/  /'
git log --oneline -2 | sed 's/^/  /'
