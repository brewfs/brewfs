#!/bin/bash
# 资源 + agent 联合巡检
set -u
export PATH=/usr/local/bin:$PATH
echo "=== $(date '+%H:%M:%S') 实例清单 ==="
for r in ap-southeast-1 cn-hangzhou; do
  out=$(aliyun ecs DescribeInstances --RegionId $r --PageSize 50 2>&1)
  ids=$(echo "$out" | grep -o '"InstanceId": "[^"]*"' | cut -d'"' -f4)
  [ -z "$ids" ] && continue
  echo "--- $r ---"
  echo "$out" | awk '
    /"InstanceName"/ {gsub(/[",]/,""); n=$2}
    /"Status"/ && !/"StoppedMode"/ {gsub(/[",]/,""); s=$2; if(n!="") {printf "  %-24s %s\n", n, s; n=""}}
  ' | head -10
done

echo
echo "=== gtxn（brewfs）==="
if tmux has-session -t gtxn 2>/dev/null; then
  echo "  会话: 存在"
  echo "  codex=$(pgrep -fc codex || echo 0) rustc=$(pgrep -cx rustc || echo 0) cargo=$(pgrep -cx cargo || echo 0)"
  a=$(stat -c%s /home/hxy/logs/gtxn.log 2>/dev/null || echo 0); sleep 6
  b=$(stat -c%s /home/hxy/logs/gtxn.log 2>/dev/null || echo 0)
  echo "  日志: $a -> $b B（$( [ "$b" -gt "$a" ] && echo 在推进 || echo 静止)）"
  tmux capture-pane -p -t gtxn 2>/dev/null | grep -vE '^\s*$' | tail -6 | cut -c1-110 | sed 's/^/    /'
else
  echo "  会话: 不存在（可能已结束）"
fi
