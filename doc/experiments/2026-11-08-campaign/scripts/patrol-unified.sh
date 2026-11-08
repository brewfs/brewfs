#!/bin/bash
# 统一巡检：tmux 里的 agent + 实验进度（给定时任务用）
set -u
K=/root/.ssh/ax-c1-20261106

echo "=== $(date '+%H:%M:%S') jump 上的 codex agent（tmux）==="
echo "  会话: $(tmux ls 2>/dev/null | tr '\n' ' ')"
A=$(stat -c%s /root/gbrew.log 2>/dev/null || echo 0); sleep 8; B=$(stat -c%s /root/gbrew.log 2>/dev/null || echo 0)
echo "  gbrew 日志: $A -> $B B（$( [ "$B" -gt "$A" ] && echo 推进 || echo 静止)）"
echo "  --- pane 尾 6 ---"
tmux capture-pane -p -t gbrew 2>/dev/null | grep -vE '^\s*$' | tail -6 | cut -c1-115 | sed 's/^/    /'

echo
echo "=== a-client 实验进度 ==="
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  目录: $(ls -1 /mnt/brewfs-c3/base 2>/dev/null | wc -l) / 1024   标记: $(ls -1 /srv/ax-data/c3/done 2>/dev/null | wc -l)"
echo "  挂载: $(mountpoint -q /mnt/brewfs-c3/base && echo y || echo n)   brewfs RSS: $(ps -C brewfs-patched -o rss= --no-headers 2>/dev/null | awk '{s+=$1} END{printf "%.0f MB", s/1024}')"
echo "  编排器: $(ls -l /srv/ax-data/c3/import-brew-final.sh 2>/dev/null | awk '{print $5" B"}' || echo 未生成)"
echo "  CSV 行数: $(wc -l < /srv/ax-data/c3/import-brew-final.csv 2>/dev/null || echo 0)"
V=$(ls -d /srv/ax-data/c3/verify-base-brew-* 2>/dev/null | tail -1)
[ -n "${V:-}" ] && echo "  verify: $(cat $V/result.json 2>/dev/null | head -c 200)"
echo "  AGENT-REPORT: $(ls -l /srv/ax-data/c3/AGENT-REPORT.md 2>/dev/null | awk '{print $5" B"}' || echo 未生成)"
EOS
