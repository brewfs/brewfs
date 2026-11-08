#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== 编译 ==="
echo "  cargo: $(pgrep -fc 'cargo build' || echo 0)  rustc: $(pgrep -fc rustc || echo 0)"
p=$(pgrep -f rustc | head -1)
[ -n "${p:-}" ] && ps -o pcpu=,etime=,rss= -p $p | sed 's/^/  rustc: /'
echo "=== 内存/swap ==="
free -g | sed 's/^/  /'
echo "=== 日志 ==="
tail -3 /root/build-new2.log 2>/dev/null | cut -c1-120 | sed 's/^/  /'
echo "=== 二进制 ==="
ls -lh /srv/ax-data/src/brewfs-new/target/release/brewfs 2>/dev/null | awk '{print "  "$5, $6, $7, $8}' || echo "  未产出"
EOS
