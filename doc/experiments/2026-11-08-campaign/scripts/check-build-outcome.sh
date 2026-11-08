#!/bin/bash
set -u
D=/mnt/brewfs-data/home/hxy/issue424
echo "=== $(date '+%H:%M:%S') 构建结局 ==="
echo "  rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)"
echo
echo "--- rebuild-cgroup.log 里的关键行 ---"
grep -aE 'build rc=|xattr rc=|错误|error\[|error:' "$D/rebuild-cgroup.log" 2>/dev/null | tail -6 | cut -c1-140 | sed 's/^/  /'
echo
echo "--- build-cgroup.log 里是否有 error ---"
grep -acE '^error' "$D/build-cgroup.log" 2>/dev/null | sed 's/^/  error 行数: /'
grep -aE '^error' "$D/build-cgroup.log" 2>/dev/null | head -5 | cut -c1-160 | sed 's/^/  /'
echo
echo "--- 最近 OOM / cgroup kill ---"
journalctl --since '-30min' --no-pager 2>/dev/null | grep -iE 'oom-kill|killed process|oom_group_kill' | tail -4 | cut -c1-150 | sed 's/^/  /' || echo "  无"
echo
echo "--- 测试二进制是否已生成 ---"
find /home/hxy/brewfs-pkv-target/release/deps -maxdepth 1 -name 'brewfs-*' -type f -newermt '-40 minutes' 2>/dev/null | head -5 | while read -r f; do
  printf "  %8.0f MB  %s\n" "$(awk "BEGIN{print $(stat -c%s "$f")/1048576}")" "$f"
done
echo
echo "--- xattr-run.log ---"
ls -l "$D/xattr-run.log" 2>/dev/null | awk '{print "  "$5"B"}' || echo "  尚未生成"
