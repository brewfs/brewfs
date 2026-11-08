#!/bin/bash
set -u
echo "=== tmux ==="
tmux ls 2>/dev/null | sed 's/^/  /'
for s in gbrew gc1; do
  echo "--- $s ---"
  if tmux has-session -t $s 2>/dev/null; then
    A=$(stat -c%s /root/$s.log 2>/dev/null || echo 0); sleep 6; B=$(stat -c%s /root/$s.log 2>/dev/null || echo 0)
    echo "  日志: $A -> $B B（$( [ "${B:-0}" -gt "${A:-0}" ] && echo 推进 || echo 静止)）"
    tail -3 /root/$s.log 2>/dev/null | cut -c1-110 | sed 's/^/    /'
  else
    echo "  ❌ 会话不存在"
  fi
done
echo
echo "=== a-client 现场 ==="
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=8 root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  ws 目录(agent c3): $(ls -1 /mnt/brewfs-c3/ws-brew 2>/dev/null | wc -l)  ws-c3: $(ls -1 /mnt/brewfs-c3/base/ws-c3-20261107 2>/dev/null | wc -l)"
echo "  c1c2 目录: $(ls -1 /srv/ax-data/c1c2 2>/dev/null | tr '\n' ' ')"
echo "  运行中脚本: $(pgrep -af 'c2-|c1-|fio|replay' | head -3 | cut -c1-100 | tr '\n' ' ')"
echo "  磁盘: $(df -h /srv/ax-data | tail -1 | awk '{print $3"/"$2}')"
echo "  RSS: $(ps -C brewfs-patched -o rss= --no-headers 2>/dev/null | awk '{s+=$1} END{printf "%.0fMB", s/1024}')"
EOS
