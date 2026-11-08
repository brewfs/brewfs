#!/bin/bash
# 每 20 分钟巡检：C3 校验进度 + C2 已完成状态 + gtxn
set -u
K=/root/.ssh/ax-c1-20261106

echo "=== $(date '+%H:%M:%S') 实例 ==="
for r in cn-hangzhou ap-southeast-1; do
  aliyun ecs DescribeInstances --RegionId $r --PageSize 20 2>&1 | awk '
    /"InstanceName"/ {gsub(/[",]/,""); n=$2}
    /"Status"/ && !/"StoppedMode"/ {gsub(/[",]/,""); s=$2; if(n!=""){printf "  %-22s %s\n", n, s; n=""}}
  '
done

echo
echo "=== 服务与实验 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS'
set -u
echo "  juicefs-c3 挂载: $(mountpoint -q /mnt/juicefs-c3 && echo yes || echo NO)"
echo "  juicefs-c2 挂载: $(mountpoint -q /mnt/juicefs-c2 && echo yes || echo NO)"
echo "  当前挂载参数: $(grep juicefs /proc/mounts | head -2 | cut -c1-90 | tr '\n' ' ')"
echo "  verify-base 进程: $(pgrep -fc 'smallfiles verify-base' || echo 0)"
echo "  replay 进程:      $(pgrep -fc full-fuse-replay || echo 0)"
V=/srv/ax-data/c3/verify-base-par-20261107
echo "  verify 目录: $(ls $V 2>/dev/null | tr '\n' ' ')"
[ -f $V/progress.json ] && echo "  progress: $(head -c 300 $V/progress.json)"
[ -f $V/result.json ] && echo "  ✅ result: $(head -c 400 $V/result.json)"
echo "  磁盘: $(df -h /mnt/juicefs-c3 2>/dev/null | tail -1 | awk '{print $3" used / "$2}')"
EOS

echo
echo "=== gtxn（brewfs，包年机）==="
if tmux has-session -t gtxn 2>/dev/null; then
  a=$(stat -c%s /home/hxy/logs/gtxn.log); sleep 5; b=$(stat -c%s /home/hxy/logs/gtxn.log)
  echo "  会话在；日志 $a -> $b B"
  tmux capture-pane -p -t gtxn 2>/dev/null | grep -vE '^\s*$' | tail -5 | cut -c1-105 | sed 's/^/    /'
else
  echo "  会话不在（可能结束）"
fi
