#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== $(date '+%H:%M:%S') gbrew agent（codex on a-client）==="
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=10 root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
A=$(stat -c%s /srv/ax-data/c3/gbrew.log 2>/dev/null || echo 0)
sleep 6
B=$(stat -c%s /srv/ax-data/c3/gbrew.log 2>/dev/null || echo 0)
echo "  日志: $A -> $B B（$( [ "$B" -gt "$A" ] && echo 在推进 || echo 静止)）"
echo "  tmux: $(tmux ls 2>/dev/null | tr '\n' ' ')"
echo "  codex 进程: $(pgrep -fc 'codex' || echo 0)"
echo "  目录数: $(ls -1 /mnt/brewfs-c3/base 2>/dev/null | wc -l) / 1024"
echo "  标记: $(ls -1 /srv/ax-data/c3/done 2>/dev/null | wc -l)"
echo "  RSS: $(ps -C brewfs-patched -o rss= --no-headers 2>/dev/null | awk '{s+=$1} END{printf "%.0f MB", s/1024}')"
echo "  --- 日志尾 8 ---"
tail -8 /srv/ax-data/c3/gbrew.log 2>/dev/null | cut -c1-125 | sed 's/^/    /'
echo "  --- AGENT-REPORT ---"
tail -6 /srv/ax-data/c3/AGENT-REPORT.md 2>/dev/null | cut -c1-140 | sed 's/^/    /'
EOS
