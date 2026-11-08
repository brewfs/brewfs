#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== 分段校验进度 ==="
ssh -i $K -o StrictHostKeyChecking=no -o ConnectTimeout=8 root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
S=/srv/ax-data/c3/segverify
echo "  已完成段: $(ls $S/seg-??.json 2>/dev/null | grep -v json$ | wc -l) / 32  (json 文件 $(ls $S/seg-*.json 2>/dev/null | wc -l))"
echo "  进程: $(pgrep -fc run-all.sh || echo 0)"
echo "  最近 3 行日志:"
tail -3 $S/run-all.log 2>/dev/null | cut -c1-150 | sed 's/^/    /'
echo "  brewfs RSS: $(ps -C brewfs-patched -o rss= --no-headers 2>/dev/null | awk '{printf "%.0f MB", $1/1024}')"
echo "  挂载: $(mountpoint -q /mnt/brewfs-c3/base && echo y || echo n)"
AGG=$S/verify-base-brew-segmented.json
[ -f "$AGG" ] && echo "  ✅ 汇总: $(head -c 300 $AGG)"
EOS
echo
echo "=== 端点 ==="
timeout 40 bash /root/probe-paths.sh 2>&1 | head -3
