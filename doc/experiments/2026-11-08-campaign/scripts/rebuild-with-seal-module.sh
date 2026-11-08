#!/bin/bash
set -u
K=/root/.ssh/ax-c1-20261106
ssh -o StrictHostKeyChecking=no -i $K root@10.20.2.164 'bash -s' <<'EOS' 2>&1 | sed 's/^/  /'
set -u
export PATH=/root/.cargo/bin:$PATH
cd /srv/ax-data/src || exit 1
echo "  seal.rs: $(wc -l < src/workspace_overlay/seal.rs 2>/dev/null || echo missing) 行"
echo "--- 重新编译 ---"
: > /root/build-seal.log
setsid nohup /root/build-seal.sh </dev/null >/dev/null 2>&1 &
sleep 30
echo "  cargo=$(pgrep -fc 'cargo build' || echo 0) rustc=$(pgrep -fc rustc || echo 0)"
tail -3 /root/build-seal.log 2>/dev/null | cut -c1-110
EOS
