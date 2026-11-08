#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>&1
set -u
echo "=== redis 是否可用 ==="
command -v redis-server 2>/dev/null || echo "  无 redis-server"
pgrep -c redis-server 2>/dev/null || echo "  redis 进程 0"
ss -ltnp 2>/dev/null | grep -c ':6379' || true
echo
echo "=== 编译 ==="
echo "  cargo: $(pgrep -fc 'cargo build' || echo 0)  rustc: $(pgrep -fc rustc || echo 0)"
tail -2 /root/build-new2.log 2>/dev/null | cut -c1-110 | sed 's/^/  /'
ls -lh /srv/ax-data/src/brewfs-new/target/release/brewfs 2>/dev/null | awk '{print "  bin "$5}' || echo "  无二进制"
EOS
