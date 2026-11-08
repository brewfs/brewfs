#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== rustc CPU / 时长 ==="
p=$(pgrep -f rustc | head -1)
if [ -n "${p:-}" ]; then
  ps -o pid=,pcpu=,etime=,rss= -p $p | sed 's/^/  /'
else
  echo "  无 rustc"
fi
echo "=== cargo ==="
pgrep -fc 'cargo build' || echo "  无 cargo"
echo "=== 日志尾 ==="
tail -3 /root/build-new.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
echo "=== 二进制 ==="
ls -lh /srv/ax-data/src/brewfs-new/target/release/brewfs 2>/dev/null | awk '{print "  "$5, $6, $7, $8}' || echo "  未产出"
echo "=== 目标目录大小 ==="
du -sh /srv/ax-data/src/brewfs-new/target 2>/dev/null | sed 's/^/  /'
EOS
