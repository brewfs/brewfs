#!/bin/bash
# 双机 agent 巡检
set -u
K=/home/hxy/.ssh/id_ed25519
SSHO="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $K"

echo "############ brewfs（g426 / 测量主机）############"
echo "  时间: $(date '+%H:%M:%S')"
echo "  codex=$(pgrep -fc 'codex exec' || echo 0) rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)"
echo "  --- acceptance 尾 4 行 ---"
tail -4 /mnt/brewfs-data/home/hxy/issue-426-20261105/acceptance.log 2>/dev/null | cut -c1-110 | sed 's/^/    /'
echo "  --- 性能结论（最新 3 条）---"
grep -ahE 'delta .*regression' /mnt/brewfs-data/home/hxy/issue-426-20261105/*.log 2>/dev/null | tail -3 | cut -c1-150 | sed 's/^/    /'
echo "  --- PR 是否创建 ---"
cd /home/hxy/brewfs-main || true
export PATH=/usr/local/bin:$PATH
gh pr list -R brewfs/brewfs --state open --limit 20 --json number,headRefName --jq '.[] | select(.headRefName|contains("426")) | "    #\(.number) \(.headRefName)"' 2>/dev/null || echo "    无"

echo
echo "############ brewfs-build（g424 / 编译主机）############"
$SSHO hxy@172.24.86.194 'bash -lc "
  echo \"  codex=\$(pgrep -fc codex || echo 0) rustc=\$(pgrep -cx rustc || echo 0) cargo=\$(pgrep -cx cargo || echo 0)\"
  echo \"  log=\$(stat -c%s /home/hxy/logs/g424.log 2>/dev/null) B\"
  echo \"  --- pane ---\"
  tmux capture-pane -p -t g424 2>/dev/null | grep -vE \"^\s*$\" | tail -8 | cut -c1-115
  echo \"  --- git ---\"
  cd /home/hxy/brewfs-main && git rev-parse --abbrev-ref HEAD && git status --porcelain | wc -l
"' 2>/dev/null | sed 's/^/  /'
