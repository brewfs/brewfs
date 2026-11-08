#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
echo "=== jump: tmux ==="
tmux ls 2>/dev/null | sed 's/^/  /'
A=$(stat -c%s /root/gport.log 2>/dev/null || echo 0); sleep 12; B=$(stat -c%s /root/gport.log 2>/dev/null || echo 0)
echo "  gport 日志: $A -> $B B"
echo
echo "=== a-client: 源码/编译状态 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.11 'bash -s' <<'EOS' 2>/dev/null
set -u
echo "  clone.rs: $([ -f /srv/ax-data/src/brewfs/src/meta/clone.rs ] && echo 有 || echo 无)"
echo "  store.rs link_many: $(grep -c 'link_many' /srv/ax-data/src/brewfs/src/meta/store.rs 2>/dev/null)"
echo "  cargo: $(pgrep -fc 'cargo' || echo 0)  rustc: $(pgrep -fc rustc || echo 0)"
tail -2 /root/build-clone.log 2>/dev/null | cut -c1-110 | sed 's/^/  /'
echo "  二进制: $(ls -lh /srv/ax-data/src/brewfs/target/release/brewfs 2>/dev/null | awk '{print $5}')"
EOS
echo
echo "=== 服务 ==="
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.12 'echo "  PD=$(pgrep -c pd-server||echo 0) TiKV=$(pgrep -c tikv-server||echo 0)"' 2>/dev/null
ssh -i $K -o StrictHostKeyChecking=no root@10.20.0.13 'echo "  RustFS=$(pgrep -c rustfs||echo 0)"' 2>/dev/null
