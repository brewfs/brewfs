#!/bin/bash
set -u
echo "=== $(date '+%H:%M:%S') #426 状态 ==="
G=/mnt/brewfs-data/home/hxy/issue-426-20261105/gates
echo "--- 门禁 exit ---"
for f in "$G"/*.exit; do
  [ -f "$f" ] || continue
  printf "  %-42s %s\n" "$(basename "$f" .exit)" "$(cat "$f" 2>/dev/null)"
done
echo
echo "--- 进程 ---"
echo "  codex=$(pgrep -fc 'codex exec' || echo 0) rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)"
ps -C rustc -o etime,pcpu,rss --no-headers 2>/dev/null | head -2 | sed 's/^/  /'
echo
echo "--- agent 日志尾 10 行 ---"
grep -av '^$' /home/hxy/logs/g426.log 2>/dev/null | tail -10 | cut -c1-140 | sed 's/^/  /'
echo
echo "--- 分支/提交 ---"
cd /home/hxy/brewfs-main || exit 1
echo "  分支: $(git rev-parse --abbrev-ref HEAD)"
git log --oneline -2 | sed 's/^/  /'
echo "  未提交: $(git status --porcelain | wc -l) 个文件"
echo
echo "--- 是否已开 PR ---"
export PATH=/usr/local/bin:$PATH
gh pr list -R brewfs/brewfs --state open --json number,title,headRefName --jq '.[] | select(.headRefName|test("426")) | "  #\(.number) \(.title[0:50])"' 2>/dev/null || echo "  无"
