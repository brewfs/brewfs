#!/bin/bash
# 10 分钟巡检：tmux agent + C3 进度（短）
set -u
K=/root/.ssh/ax-c1-20261106

echo "=== $(date '+%H:%M:%S') agent（jump tmux）==="
echo "  会话: $(tmux ls 2>/dev/null | tr '\n' ' ')"
A=$(stat -c%s /root/gbrew.log 2>/dev/null || echo 0); sleep 8; B=$(stat -c%s /root/gbrew.log 2>/dev/null || echo 0)
echo "  gbrew 日志: $A -> $B B（$( [ "$B" -gt "$A" ] && echo 推进 || echo 静止)）"
echo "  pane 尾 3: $(tmux capture-pane -p -t gbrew 2>/dev/null | grep -vE '^\s*$' | tail -3 | tr '\n' ' ' | cut -c1-160)"

echo
echo "=== a-client 进度 ==="
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=8 root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  挂载: $(mountpoint -q /mnt/brewfs-c3/base && echo y || echo n)  brewfs: $(pgrep -fc brewfs-patched || echo 0)  RSS: $(ps -C brewfs-patched -o rss= --no-headers 2>/dev/null | awk '{s+=$1} END{printf "%.0fMB", s/1024}')"
echo "  base 顶层: $(ls -1 /mnt/brewfs-c3/base 2>/dev/null | wc -l)   small/d*: $(ls -1 /mnt/brewfs-c3/base/small 2>/dev/null | wc -l)   done 标记: $(ls -1 /srv/ax-data/c3/done 2>/dev/null | wc -l)"
echo "  verify 进程: $(pgrep -fc 'smallfiles verify-base' || echo 0)"
V=$(ls -dt /srv/ax-data/c3/verify-base-brew-* 2>/dev/null | head -1)
[ -n "${V:-}" ] && echo "  最新 verify 目录: $(basename $V) → $(ls -A $V 2>/dev/null | tr '\n' ' ')" && { [ -f "$V/result.json" ] && echo "  ✅ $(head -c 200 $V/result.json)"; }
P=$(pgrep -f 'smallfiles verify-base' | head -1)
[ -n "${P:-}" ] && echo "  verify etime: $(ps -o etime= -p $P | tr -d ' ')"
echo "  AGENT-REPORT: $(stat -c%s /srv/ax-data/c3/AGENT-REPORT.md 2>/dev/null || echo 0) B"
EOS
