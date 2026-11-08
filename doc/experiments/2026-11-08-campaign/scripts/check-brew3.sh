#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== ws-r1 克隆进度 ==="
if [ -d /mnt/brewfs-c3/base/ws-r1/small ]; then
  echo "  d* 目录数: $(ls /mnt/brewfs-c3/base/ws-r1/small 2>/dev/null | wc -l) / 1024"
else
  echo "  ws-r1/small 还不存在"
fi
echo "  c3cmp2 内容: $(ls /srv/ax-data/c3cmp2 2>/dev/null | tr '\n' ' ')"
echo "  smallfiles 进程: $(pgrep -fc smallfiles || echo 0)"
echo "  cp 进程: $(pgrep -fc 'cp -al' || echo 0)"
echo "  brewfs RSS: $(ps -C brewfs-patched -o rss= --no-headers 2>/dev/null | awk '{printf "%.0fMB", $1/1024}')"
echo
echo "=== 日志尾 ==="
tail -4 /srv/ax-data/c3cmp2/run.log 2>/dev/null | cut -c1-110 | sed 's/^/  /'
tail -2 /srv/ax-data/c3cmp2/logs/ws.err 2>/dev/null | cut -c1-110 | sed 's/^/  err: /'
EOS
