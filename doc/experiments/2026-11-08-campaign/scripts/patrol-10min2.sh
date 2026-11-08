#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== $(date '+%H:%M:%S') agent（jump tmux）==="
echo "  会话: $(tmux ls 2>/dev/null | tr '\n' ' ')"
A=$(stat -c%s /root/gbrew.log 2>/dev/null || echo 0); sleep 10; B=$(stat -c%s /root/gbrew.log 2>/dev/null || echo 0)
echo "  日志: $A -> $B B（$( [ "${B:-0}" -gt "${A:-0}" ] && echo 推进 || echo 静止)）"
echo "  pane 尾 5:"
tmux capture-pane -p -t gbrew 2>/dev/null | grep -vE '^\s*$' | tail -5 | cut -c1-120 | sed 's/^/    /'
echo "  codex 进程: $(pgrep -fc 'codex exec' || echo 0)"

echo
echo "=== a-client ==="
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=8 root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  内存: $(free -g | awk '/Mem:/{print $3"G used / "$7"G avail"}')"
echo "  挂载: $(mountpoint -q /mnt/brewfs-c3/base && echo y || echo n)  brewfs: $(pgrep -fc brewfs-patched || echo 0)  RSS: $(ps -C brewfs-patched -o rss= --no-headers 2>/dev/null | awk '{s+=$1} END{printf "%.0fMB", s/1024}')"
echo "  校验进程: $(pgrep -af 'smallfiles|verify' | head -2 | cut -c1-100 | tr '\n' ' ')"
echo "  verify 目录: $(ls -d /srv/ax-data/c3/verify-base-brew-* 2>/dev/null | wc -l) 个；最新: $(ls -dt /srv/ax-data/c3/verify-base-brew-* 2>/dev/null | head -1)"
echo "  AGENT-REPORT: $(stat -c%s /srv/ax-data/c3/AGENT-REPORT.md 2>/dev/null || echo 0) B"
echo "  pilot: $(ls -d /srv/ax-data/c3/pilot-brew-* 2>/dev/null | tail -1)"
EOS
