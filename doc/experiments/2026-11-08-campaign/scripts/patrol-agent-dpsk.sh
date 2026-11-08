#!/bin/bash
set -u
echo "=== agent（jump）==="
echo "  会话: $(tmux ls 2>/dev/null | tr '\n' ' ')"
A=$(stat -c%s /root/gbrew.log 2>/dev/null || echo 0); sleep 8; B=$(stat -c%s /root/gbrew.log 2>/dev/null || echo 0)
echo "  日志: $A -> $B B（$( [ "${B:-0}" -gt "${A:-0}" ] && echo 推进 || echo 静止)）"
echo "  codex 进程: $(pgrep -fc 'codex exec' || echo 0)"
tail -4 /root/gbrew.log 2>/dev/null | cut -c1-120 | sed 's/^/    /'
echo
echo "=== a-client ==="
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=8 root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  ws 目录: $(ls -1 /mnt/brewfs-c3/ws-brew 2>/dev/null | wc -l) / 1024"
echo "  物化进程: $(pgrep -fc materialize-ws || echo 0)"
echo "  AGENT-REPORT: $(stat -c%s /srv/ax-data/c3/AGENT-REPORT.md 2>/dev/null || echo 0) B"
echo "  pilot 目录: $(ls -d /srv/ax-data/c3/pilot-brew-* 2>/dev/null | wc -l)"
echo "  RSS: $(ps -C brewfs-patched -o rss= --no-headers 2>/dev/null | awk '{s+=$1} END{printf "%.0fMB", s/1024}')"
echo "  挂载: $(mountpoint -q /mnt/brewfs-c3/base && echo y || echo n)"
EOS
