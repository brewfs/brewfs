#!/bin/bash
set -u
G=/mnt/brewfs-data/home/hxy/issue-426-20261105/gates
echo "=== $(date '+%H:%M:%S') 426 门禁日志 ==="
ls -lt "$G" 2>/dev/null | head -8 | sed 's/^/  /'
echo
echo "--- 最新门禁日志尾部（看红测试是否按预期失败）---"
LATEST=$(ls -t "$G"/*.log 2>/dev/null | head -1)
echo "  $LATEST"
if [ -n "${LATEST:-}" ]; then
  grep -aE 'test result:|FAILED|panicked|assertion|error\[|assert_eq' "$LATEST" 2>/dev/null | tail -8 | cut -c1-160 | sed 's/^/  /'
  echo "  --- 尾 8 行 ---"
  tail -8 "$LATEST" | cut -c1-160 | sed 's/^/  /'
fi
echo
echo "--- git 改动 ---"
cd /home/hxy/brewfs-main || exit 1
git status --porcelain | head -8 | sed 's/^/  /'
echo "  分支: $(git rev-parse --abbrev-ref HEAD)"
echo
echo "--- rustc / unit ---"
systemctl list-units 'issue426-*' --no-pager --plain 2>/dev/null | head -4 | sed 's/^/  /'
ps -C rustc -o etime,pcpu,rss --no-headers 2>/dev/null | head -2 | sed 's/^/  /'
